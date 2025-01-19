import socket
import threading
import json
import time
import uuid
import weakref
import signal
import argparse

CONFIG_FILE = "config.json"
BUFFER_SIZE = 1024

registry = {}
registry_lock = threading.Lock()
stop_event = threading.Event()


def load_config():
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config: {e}")
        raise SystemExit(1)


def send_message(host_with_port, message):
    try:
        host, port = host_with_port.split(":")
        with socket.create_connection((host, int(port)), timeout=5) as sock:
            sock.sendall(json.dumps(message).encode())
    except socket.error as e:
        print(f"Error connecting to {host_with_port}: {e}")


def send_message_to_all(message):
    known_hosts = load_config().get("known_hosts", [])
    for host_with_port in known_hosts:
        send_message(host_with_port, message)


def receive_message(sock):
    try:
        chunks = []
        while True:
            chunk = sock.recv(BUFFER_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
        return json.loads(b"".join(chunks).decode())
    except (socket.error, json.JSONDecodeError) as e:
        print(f"Error receiving or parsing message: {e}")
        return {}


class BaseAgent:
    def __init__(self, home_host_with_port: str):
        self.home_host_with_port = home_host_with_port
        self.id = str(uuid.uuid4())
        self.pending_tasks = {}  # task_id -> (expected_results, results)
        with registry_lock:
            registry[self.id] = weakref.ref(self)

    def __del__(self):
        with registry_lock:
            registry.pop(self.id, None)

    def get_data(self):
        return {}

    def on_arrive(self, data, source_host):
        pass

    def result_received(self, task_id, source_host, result):
        if task_id in self.pending_tasks:
            expected_results, results = self.pending_tasks[task_id]
            results.append(result)

            if len(results) == expected_results:
                self.on_all_results(task_id, results)
                del self.pending_tasks[task_id]

    def on_all_results(self, task_id, results):
        results_formatted = json.dumps(results, indent=2)
        print(f"All results received for task {task_id}:\n{results_formatted}")

    def move_to(self, host_with_port):
        task_id = str(uuid.uuid4())
        self.pending_tasks[task_id] = (1, [])
        message = {
            "type": "move",
            "data": self.get_data(),
            "source": self.home_host_with_port,
            "id": self.id,
            "task_id": task_id,
        }
        send_message(host_with_port, message)

    def move_to_all(self):
        task_id = str(uuid.uuid4())
        known_hosts = load_config().get("known_hosts", [])
        self.pending_tasks[task_id] = (len(known_hosts), [])
        message = {
            "type": "move",
            "data": self.get_data(),
            "source": self.home_host_with_port,
            "id": self.id,
            "task_id": task_id,
        }
        send_message_to_all(message)


class TimeAgent(BaseAgent):
    def get_data(self):
        return {"request": "time", "agent_type": "TimeAgent"}

    def move_to_all(self):
        print("Requesting time from all known hosts")
        super().move_to_all()

    def on_arrive(self, data, source_host):
        if data.get("request") == "time":
            print("Received time request, returning current time")
            time.sleep(10)  # Simulate processing delay
            return {"server": socket.gethostname(), "time": time.ctime()}
        return None

    def on_all_results(self, task_id, results):
        super().on_all_results(task_id, results)
        time_results = sorted(
            (result for result in results if not result.get("is_error")),
            key=lambda x: x["data"]["time"],
        )
        if time_results:
            min_time = time_results[0]["data"]["time"]
            max_time = time_results[-1]["data"]["time"]
            min_time_server = time_results[0]["data"]["server"]
            max_time_server = time_results[-1]["data"]["server"]
            time_diff = time.mktime(time.strptime(max_time)) - time.mktime(
                time.strptime(min_time)
            )
            print(
                f"Time difference between {min_time_server} and {max_time_server}: {time_diff} seconds"
            )


AGENT_CLASSES = {"TimeAgent": TimeAgent}


def handle_incoming_messages(port, host_with_port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", port))
        sock.listen(5)
        print(f"Server listening on port {port}")

        while not stop_event.is_set():
            try:
                sock.settimeout(1)
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            except socket.error as e:
                print(f"Socket error: {e}")
                break

            client_thread = threading.Thread(
                target=handle_client_connection,
                args=(conn, addr, host_with_port),
                daemon=True,
            )
            client_thread.start()


def handle_client_connection(conn, addr, host_with_port):
    with conn:
        # try:
        message = receive_message(conn)
        if not message or "type" not in message:
            print(f"Invalid message received from {addr}")
            return

        if message["type"] == "move":
            handle_move_message(message, host_with_port)
        elif message["type"] in {"result", "error"}:
            handle_result_or_error_message(message)
        # except Exception as e:
        #    print(f"Error handling client {addr}: {e}")


def handle_move_message(message, host_with_port):
    agent_data = message["data"]
    source_host = message["source"]
    agent_type = agent_data.get("agent_type", "BaseAgent")
    agent_class = AGENT_CLASSES.get(agent_type, BaseAgent)
    agent = agent_class(home_host_with_port=host_with_port)

    try:
        result = agent.on_arrive(agent_data, source_host)
        if result:
            send_message(
                source_host,
                {
                    "type": "result",
                    "data": result,
                    "source": host_with_port,
                    "id": message["id"],
                    "task_id": message["task_id"],
                    "is_error": False,
                },
            )
    except Exception as e:
        send_message(
            source_host,
            {
                "type": "error",
                "error": str(e),
                "source": host_with_port,
                "id": message["id"],
                "task_id": message["task_id"],
                "is_error": True,
            },
        )


def handle_result_or_error_message(message):
    with registry_lock:
        agent_ref = registry.get(message["id"])
        if agent_ref:
            agent = agent_ref()
            if agent:
                agent.result_received(message["task_id"], message["source"], message)


def shutdown_handler(signum, frame):
    print("Shutdown signal received.")
    stop_event.set()


def parse_args():
    parser = argparse.ArgumentParser(description="Run the agent server.")
    parser.add_argument("--port", type=int, default=50000, help="Port to listen on")
    parser.add_argument(
        "--client", action="store_true", help="Start the server as a client"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    MESSAGE_PORT = args.port

    conf = load_config()
    host_with_port = f"{conf['host']}:{MESSAGE_PORT}"

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    server_thread = threading.Thread(
        target=handle_incoming_messages,
        args=(MESSAGE_PORT, host_with_port),
        daemon=True,
    )
    server_thread.start()

    if args.client:
        print(f"Started as a client at {host_with_port}")
    else:
        print(
            f"Started as a server at {host_with_port}, sending time agent to all known hosts"
        )
        agent = TimeAgent(host_with_port)
        agent.move_to_all()

    try:
        print("Press Ctrl+C to stop")
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown_handler(None, None)

    server_thread.join()
    print("Exiting...")
