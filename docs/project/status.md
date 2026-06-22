# Status And Limitations

`paglets` is an early Python mobile-object runtime for experiments and local/trusted meshes. Hosts must already have the same paglet code importable; movement sends class names and dataclass state, not Python code, stacks, threads, sockets, or arbitrary live resources.

Current intentional limitations:

- No code upload or sandbox for untrusted code.
- Paglet classes and state classes must be importable by `module:qualname`.
- Active paglets run in child processes; startup and IPC overhead are real.
- Message handling is serial per paglet. Batch tiny high-frequency work.
- Flat root imports are intentionally unsupported.

Use API-key authentication for shared networks and relay deployments.
