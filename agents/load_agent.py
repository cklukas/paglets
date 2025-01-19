import os
import time
from agents.base_agent import BaseAgent


class LoadAgent(BaseAgent):
    def get_data(self):
        return {"request": "load", "agent_type": "LoadAgent"}

    def move_to_all(self):
        print("Requesting system load from all known hosts")
        super().move_to_all()

    def on_arrive(self, data, source_host):
        if data.get("request") == "load":
            print(f"Received load request from {source_host}, returning system load")
            time.sleep(5)  # Simulate processing delay
            load_avg = os.getloadavg()  # Get system load averages (1, 5, 15 minutes)
            return {
                "server": self.home_host_with_port,
                "load": load_avg[0],  # Report the 1-minute load average
            }
        return None

    def on_all_results(self, task_id, results):
        super().on_all_results(task_id, results)

        load_results = sorted(
            (result for result in results if not result.get("is_error")),
            key=lambda x: x["data"]["load"],
        )

        if load_results:
            min_load = load_results[0]["data"]["load"]
            max_load = load_results[-1]["data"]["load"]
            min_load_server = load_results[0]["data"]["server"]
            max_load_server = load_results[-1]["data"]["server"]

            print(f"Lowest load: {min_load} on {min_load_server}")
            print(f"Highest load: {max_load} on {max_load_server}")
