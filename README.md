# PAglets

Python library (currently just a first draft code), loosly inspired by Java Aglets.

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
Server listening on port 50001
Press Ctrl+C to stop
Received time info from mac-studio.lan: Sat Jan 18 20:59:00 2025
Received time info from mac-studio.lan: Sat Jan 18 20:59:00 2025
```