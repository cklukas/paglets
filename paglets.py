import socket
import threading
import json
import time
import uuid
import weakref
import signal
import argparse

CONFIG_FILE = 'config.json'
BUFFER_SIZE = 1024

registry = {}
registry_lock = threading.Lock()
stop_event = threading.Event()


def load_config():
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config: {e}")
        raise SystemExit(1)


def send_message(host, port, message):
    try:
        with socket.create_connection((host, port), timeout=5) as sock:
            sock.sendall(message.encode())
    except socket.error as e:
        print(f"Error connecting to {host}:{port}: {e}")


def send_message_to_all(message, port):
    known_hosts = load_config().get("known_hosts", [])
    for host_with_port in known_hosts:
        host, host_port = host_with_port.split(":")
        send_message(host, int(host_port), message)

def receive_message(sock):
    try:
        chunks = []
        while True:
            chunk = sock.recv(BUFFER_SIZE)
            if not chunk:
                break
            chunks.append(chunk)
        return b''.join(chunks).decode()
    except socket.error as e:
        print(f"Error receiving message: {e}")
        return ""


class BaseAgent:
    def __init__(self):
        self.config = load_config()
        self.id = str(uuid.uuid4())
        with registry_lock:
            registry[self.id] = weakref.ref(self)

    def __del__(self):
        with registry_lock:
            registry.pop(self.id, None)

    def get_data(self):
        return json.dumps({})

    def on_arrive(self, data, source_host):
        pass

    def result_received(self, source_host, result, error):
        pass

    def move_to(self, host):
        data = self.get_data()
        message = json.dumps({"type": "move", "data": data, "source": self.config["host"], "id": self.id})
        send_message(host, message)

    def move_to_all(self):
        data = self.get_data()
        message = json.dumps({"type": "move", "data": data, "source": self.config["host"], "id": self.id})
        send_message_to_all(message, self.config["port"])

class TimeAgent(BaseAgent):
    def get_data(self):
        return json.dumps({"request": "time", "agent_type": "TimeAgent"})

    def on_arrive(self, data, source_host):
        if data.get("request") == "time":
            return json.dumps({"server": socket.gethostname(), "time": time.ctime()})
        return None

    def result_received(self, source_host, result, error):
        if not error:
            try:
                result_data = json.loads(result)
                print(f"Received time info from {result_data['server']}: {result_data['time']}")
            except json.JSONDecodeError:
                print(f"Invalid result data from {source_host}: {result}")
        else:
            print(f"Error received from {source_host}: {result}")


AGENT_CLASSES = {
    "TimeAgent": TimeAgent
}


def handle_incoming_messages(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(('', port))
        sock.listen(5)
        print(f"Server listening on port {MESSAGE_PORT}")

        while not stop_event.is_set():
            try:
                sock.settimeout(1)
                conn, addr = sock.accept()
            except socket.timeout:
                continue
            except socket.error as e:
                print(f"Socket error: {e}")
                break

            with conn:
                try:
                    message = json.loads(receive_message(conn))
                    if "type" not in message:
                        print("Invalid message received")
                        continue
                    if message["type"] == "move":
                        handle_move_message(message)
                    elif message["type"] in {"result", "error"}:
                        handle_result_or_error_message(message)
                except json.JSONDecodeError as e:
                    print(f"Failed to parse message: {e}")


def handle_move_message(message):
    agent_data = json.loads(message["data"])
    source_host = message["source"]
    agent_type = agent_data.get("agent_type", "BaseAgent")
    agent_class = AGENT_CLASSES.get(agent_type, BaseAgent)
    agent = agent_class()

    try:
        result = agent.on_arrive(agent_data, source_host)
        if result:
            result_message = json.dumps({
                "type": "result",
                "data": result,
                "source": agent.config["host"],
                "id": message["id"],
                "is_error": False
            })
            send_message(source_host, int(agent.config["port"]), result_message)
    except Exception as e:
        error_message = json.dumps({
            "type": "error",
            "error": str(e),
            "source": agent.config["host"],
            "id": message["id"],
            "is_error": True
        })
        send_message(source_host, int(agent.config["port"]), error_message)


def handle_result_or_error_message(message):
    with registry_lock:
        agent_ref = registry.get(message["id"])
        if agent_ref:
            agent = agent_ref()
            if agent:
                if message["type"] == "result":
                    agent.result_received(message["source"], message["data"], False)
                elif message["type"] == "error":
                    agent.result_received(message["source"], message["data"], True)


def shutdown_handler(signum, frame):
    print("Shutdown signal received.")
    stop_event.set()

def parse_args():
    parser = argparse.ArgumentParser(description="Run the agent server.")
    parser.add_argument("--port", type=int, default=50000, help="Port to listen on")
    parser.add_argument("--client", action="store_true", help="Start the server as a client")
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    MESSAGE_PORT = args.port

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Start the server thread
    server_thread = threading.Thread(target=handle_incoming_messages, args=(MESSAGE_PORT,), daemon=True)
    server_thread.start()

    # if started as a client just print a info message
    if args.client:
        print("Started as a client")
    else:
        print("Started as a server, sending time agent to all known hosts")
        agent = TimeAgent()
        agent.move_to_all()

    try:
        print("Press Ctrl+C to stop")
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown_handler(None, None)

    server_thread.join()
    print("Exiting...")
