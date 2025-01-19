import json
import uuid

from paglets.util import (
    forget_agent,
    load_config,
    memorize_agent,
    send_message,
    send_message_to_all,
)


class BaseAgent:
    def __init__(self, home_host_with_port: str):
        self.home_host_with_port = home_host_with_port
        self.id = str(uuid.uuid4())
        self.pending_tasks = {}  # task_id -> (expected_results, results)
        memorize_agent(self)

    def __del__(self):
        forget_agent(self)

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
