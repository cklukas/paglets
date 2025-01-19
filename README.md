# PAglets

Python library (currently just a first draft code), loosely inspired by Java Aglets.

## Test

Start second instance first (the client which just 'listens'):

`python3 main.py --port 50002 --client`

Then start first instance (the server which sends the time agent):

`python3 main.py --port 50001`

### Example output

Client output:

```bash
$  python3 main.py --port 50002 --client
Started as a client at 127.0.0.1:50002
Press Ctrl+C to stop
Server listening on port 50002
Received time request from 127.0.0.1:50001, returning current time
```


Main output:

```bash
$ python3 main.py --port 50001
Started as a server at 127.0.0.1:50001, sending time agent to all known hosts
Requesting time from all known hosts
Server listening on port 50001
Received time request from 127.0.0.1:50001, returning current time
Press Ctrl+C to stop
All results received for task a58393af-c92f-4461-a3ee-6748b06a85ae:
[
  {
    "type": "result",
    "data": {
      "server": "127.0.0.1:50001",
      "time": "Sun Jan 19 12:41:15 2025"
    },
    "source": "127.0.0.1:50001",
    "id": "04180ab8-975f-453d-a05e-9be19721e2be",
    "task_id": "a58393af-c92f-4461-a3ee-6748b06a85ae",
    "is_error": false
  },
  {
    "type": "result",
    "data": {
      "server": "127.0.0.1:50002",
      "time": "Sun Jan 19 12:41:15 2025"
    },
    "source": "127.0.0.1:50002",
    "id": "04180ab8-975f-453d-a05e-9be19721e2be",
    "task_id": "a58393af-c92f-4461-a3ee-6748b06a85ae",
    "is_error": false
  }
]
Time difference between 127.0.0.1:50001 and 127.0.0.1:50002: 0.0 seconds
```
