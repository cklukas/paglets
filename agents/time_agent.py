import time

from agents.base_agent import BaseAgent


class TimeAgent(BaseAgent):
    def get_data(self):
        return {"request": "time", "agent_type": "TimeAgent"}

    def move_to_all(self):
        print("Requesting time from all known hosts")
        super().move_to_all()

    def on_arrive(self, data, meta_data, source_host):
        if data.get("request") == "time":
            print(f"Received time request from {source_host}, returning current time")
            time.sleep(10)  # Simulate processing delay
            return {"server": self.home_host_with_port, "time": time.ctime()}
        return None

    def on_all_results(self, task_id, result_data, result_meta_data):
        print(f"All results received for task {task_id}")
        time_results = sorted(
            (result for result in result_data if not result.get("is_error")),
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
                f"Time difference: {max_time_server} is {time_diff} seconds ahead of {min_time_server}"
            )
