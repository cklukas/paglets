# Source-Tree Demos

The repository also has simple runnable demos under the top-level `demos/`
directory. These are not installed as packaged example modules; they are small
scripts meant for reading and experimentation from a checkout:

```bash
uv run python demos/disk_survey_demo.py --hosts alpha beta gamma
uv run python demos/clone_workers_demo.py
uv run python demos/itinerary_demo.py
uv run python demos/message_patterns_demo.py
```

The bundled scripts re-import themselves as `demos.<module>` before creating
paglets, so their classes are importable by spawned child processes. For your
own examples, keep paglet classes in importable modules and let the script entry
point only call into that module.

Use packaged examples when you want installed CLI commands or importable example
agents. Use source-tree demos when you want compact scripts that illustrate one
runtime concept at a time.
