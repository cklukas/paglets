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
$ python3 main.py --port 50002 --client
Started as a client
Press Ctrl+C to stop
Server listening on port 50002
```


Main output:

```bash
$ python3 main.py --port 50001

Started as a server, sending time agent to all known hosts
Requesting time from all known hosts
Server listening on port 50001
Received time request, returning current time
Press Ctrl+C to stop
All results received for task e3caa67b-c1e4-4cc3-a1e3-40d6ecac846c:
[
  {
    "type": "result",
    "data": {
      "server": "mac-studio.lan",
      "time": "Sun Jan 19 12:22:39 2025"
    },
    "source": "127.0.0.1:50001",
    "id": "9bee2d1b-834c-46ff-898f-fd33d846bad3",
    "task_id": "e3caa67b-c1e4-4cc3-a1e3-40d6ecac846c",
    "is_error": false
  },
  {
    "type": "result",
    "data": {
      "server": "mac-studio.lan",
      "time": "Sun Jan 19 12:22:39 2025"
    },
    "source": "127.0.0.1:50002",
    "id": "9bee2d1b-834c-46ff-898f-fd33d846bad3",
    "task_id": "e3caa67b-c1e4-4cc3-a1e3-40d6ecac846c",
    "is_error": false
  }
]
Time difference between mac-studio.lan and mac-studio.lan: 0.0 seconds
```
