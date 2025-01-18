# paglets

Python library, inspired by Java Aglets.

## test

Start second instance first (the client which just 'listens'):

`python3 paglets.py --port 50002 --client`

Then start first instance (the server which sends the time agent):

`python3 paglets.py --port 50001`