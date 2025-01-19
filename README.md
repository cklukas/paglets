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
$  % python3 main.py --port 50002 --client
Started as a client at 127.0.0.1:50002
Press Ctrl+C to stop
Server listening on port 50002
Received time request from 127.0.0.1:50001, returning current time
Received load request from 127.0.0.1:50001, returning system load
Received dataframe generation request from 127.0.0.1:50001
```


Main output:

```bash
$ python3 main.py --port 50001
Started as a server at 127.0.0.1:50001, sending time agent to all known hosts
Requesting time from all known hosts
Server listening on port 50001
Requesting system load from all known hosts
Received time request from 127.0.0.1:50001, returning current time
Received load request from 127.0.0.1:50001, returning system load
Requesting dataframe generation from all known hosts
Received dataframe generation request from 127.0.0.1:50001
Press Ctrl+C to stop
All results received for task 0451286e-2c61-48b9-af11-b587b1fba3a5
Received dataframe from 127.0.0.1:50002 with dimensions (24144, 132)
Size of received data: 62 MB
Received dataframe from 127.0.0.1:50001 with dimensions (20607, 249)
Size of received data: 99 MB
All results received for task 63a71cc5-fefc-40f5-9c8c-76df015cfd7b
Lowest load: 1.82 on 127.0.0.1:50001
Highest load: 1.82 on 127.0.0.1:50002
All results received for task 3555e531-36f0-44cf-9d88-e21cf5018cc5
Time difference: 127.0.0.1:50002 is 0.0 seconds ahead of 127.0.0.1:50001
```
