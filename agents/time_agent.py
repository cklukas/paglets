from agents.base_agent import BaseAgent
import time
import socket


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
