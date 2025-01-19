import threading
import socket
import threading
import json
import time
import weakref

CONFIG_FILE = "config.json"
BUFFER_SIZE = 1024

registry = {}
registry_lock = threading.Lock()
stop_event = threading.Event()


def memorize_agent(agent):
    with registry_lock:
        registry[agent.id] = weakref.ref(agent)


def forget_agent(agent):
    with registry_lock:
        registry.pop(agent.id, None)


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


def handle_incoming_messages(AGENT_CLASSES, port, host_with_port):
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
                args=(AGENT_CLASSES, conn, addr, host_with_port),
                daemon=True,
            )
            client_thread.start()


def handle_client_connection(AGENT_CLASSES, conn, addr, host_with_port):
    with conn:
        # try:
        message = receive_message(conn)
        if not message or "type" not in message:
            print(f"Invalid message received from {addr}")
            return

        if message["type"] == "move":
            handle_move_message(AGENT_CLASSES, message, host_with_port)
        elif message["type"] in {"result", "error"}:
            handle_result_or_error_message(message)
        # except Exception as e:
        #    print(f"Error handling client {addr}: {e}")


def handle_move_message(AGENT_CLASSES, message, host_with_port):
    agent_data = message["data"]
    source_host = message["source"]
    agent_type = agent_data.get("agent_type", "BaseAgent")
    agent_class = AGENT_CLASSES.get(agent_type, None)
    if not agent_class:
        print(f"Unknown agent type: {agent_type}")
        return

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


def load_config():
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error loading config: {e}")
        raise SystemExit(1)


def receive_message(sock):
    try:
        message = bytearray()
        while True:
            chunk = sock.recv(BUFFER_SIZE)
            if not chunk:
                break
            message.extend(chunk)
        return json.loads(message.decode())
    except (socket.error, json.JSONDecodeError) as e:
        print(f"Error receiving or parsing message: {e}")
        return {}


def wait_for_exit():
    try:
        print("Press Ctrl+C to stop")
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        shutdown_handler(None, None)
