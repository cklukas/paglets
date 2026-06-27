# Local Multi-Host Development

Multiple hosts can run on one machine by using different ports. This is the fastest way to test movement, cloning, services, and mesh discovery.

```bash
uv run paglets host --name alpha --port 8765 --mesh-version dev
uv run paglets host --name beta --port 8766 --peer http://127.0.0.1:8765 --mesh-version dev
```

Same-host movement bypasses HTTP inside one host process. Movement between two local host processes still uses the normal HTTP transfer path over loopback.

Run source-tree demos from `demos/`:

```bash
uv run python demos/disk_survey_demo.py --hosts alpha beta gamma
uv run python demos/clone_workers_demo.py
uv run python demos/itinerary_demo.py
```

Packaged example CLIs discover an entry host automatically unless `--entry` is supplied.
