# PAglets

Python library (currently just a first draft code), loosely inspired by Java Aglets.

## Test

Start second instance first (the client which just 'listens'):

`python3 paglets.py --port 50002 --client`

Then start first instance (the server which sends the time agent):

`python3 paglets.py --port 50001`

### Example output

Client output:

```bash
$ python3 paglets.py --port 50002 --client
Started as a client
Press Ctrl+C to stop
Server listening on port 50002
```


Main output:

```bash
$ python3 paglets.py --port 50001

Started as a server, sending time agent to all known hosts
Requesting time from all known hosts
Server listening on port 50001
Received time request, returning current time
Press Ctrl+C to stop
All results received for task a95400e6-d904-4034-8bf8-8a3229c86c49:
[
  {
    "type": "result",
    "data": {
      "server": "mac-studio.lan",
      "time": "Sun Jan 19 09:41:33 2025"
    },
    "source": "127.0.0.1:50001",
    "id": "5475422e-c86e-4fc6-8f01-61310bf8a840",
    "task_id": "a95400e6-d904-4034-8bf8-8a3229c86c49",
    "is_error": false
  },
  {
    "type": "result",
    "data": {
      "server": "mac-studio.lan",
      "time": "Sun Jan 19 09:41:33 2025"
    },
    "source": "127.0.0.1:50001",
    "id": "5475422e-c86e-4fc6-8f01-61310bf8a840",
    "task_id": "a95400e6-d904-4034-8bf8-8a3229c86c49",
    "is_error": false
  }
]
Time difference between mac-studio.lan and mac-studio.lan: 0.0 seconds
```
