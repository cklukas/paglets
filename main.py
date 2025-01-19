import threading
import signal
import argparse

from agents.time_agent import TimeAgent
from paglets.util import (
    handle_incoming_messages,
    load_config,
    shutdown_handler,
    wait_for_exit,
)


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

    AGENT_CLASSES = {"TimeAgent": TimeAgent}

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    server_thread = threading.Thread(
        target=handle_incoming_messages,
        args=(AGENT_CLASSES, MESSAGE_PORT, host_with_port),
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

    wait_for_exit()

    server_thread.join()
    print("Exiting...")
