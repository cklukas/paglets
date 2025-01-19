import pandas as pd
import numpy as np
import json
import sys

from agents.base_agent import BaseAgent


class DataframeAgent(BaseAgent):
    def get_data(self):
        return {"request": "generate_dataframe", "agent_type": "DataframeAgent"}

    def move_to_all(self):
        print("Requesting dataframe generation from all known hosts")
        super().move_to_all()

    def on_arrive(self, data, source_host):
        if data.get("request") == "generate_dataframe":
            print(f"Received dataframe generation request from {source_host}")

            # Generate a random dataframe with random dimensions
            rows = np.random.randint(10000, 50000)
            cols = np.random.randint(100, 500)
            df = pd.DataFrame(
                np.random.random((rows, cols)),
                columns=[f"col_{i}" for i in range(cols)],
            )

            # Serialize the dataframe to JSON for transfer
            df_json = df.to_json(orient="split")

            return {
                "server": self.home_host_with_port,
                "dataframe": json.loads(df_json),  # Send as dictionary
                "rows": rows,
                "cols": cols,
            }
        return None

    def on_all_results(self, task_id, results):
        print(f"All results received for task {task_id}")
        for result in results:
            if not result.get("is_error"):
                server = result["data"]["server"]
                rows = result["data"]["rows"]
                cols = result["data"]["cols"]

                # Calculate the size of the received JSON dictionary
                dataframe_dict = result["data"]["dataframe"]
                json_size_mb = sys.getsizeof(json.dumps(dataframe_dict)) / 1024 / 1024

                print(
                    f"Received dataframe from {server} with dimensions ({rows}, {cols})"
                )
                print(f"Size of received data: {json_size_mb:.0f} MB")
