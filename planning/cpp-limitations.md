<!-- Copyright (c) 2026 by C. Klukas.
     Licensed under the MIT License. See LICENSE for details. -->

# Documented C++ limitations

These remain supported-behaviour limits or optional improvements. They are
not evidence that every bug has been fixed; see [Remaining C++ issues](cpp-paper-audit.md).

- Windows compute jobs are interpreted; host-side AOT compilation is not
  implemented there. The default Windows preset leaves the AOT loader off.
- CLI `remote call` and `remote send` address the connected host. After a
  paglet has left, use `remote locate` and connect to its current host.
- CLI launches use empty manifests and have no owner-signed grant-request
  command. Those workflows use the embedding and ledger APIs.
- Resident paglets are created through the runtime API. CLI/web controls
  do not choose resident trust or externally deactivate a paglet; paglets
  can request deactivation themselves.
- CLI dispatch waits at most 120 s. A pending answer can precede the move's
  eventual completion; use `remote locate` to check its current host.
- Continuing jobs return results after movement, but status/cancel ownership
  is not handed to the paglet's new host.
- `granted_dir` returns `bad_state` while human approval is pending; there is
  no variant that waits for the later grant notification.
- The grant/audit ledger is append-only, without deduplication or retention.
- Compute-slot leases account for cooperative work; expiry releases slots
  without preempting CPU execution. Native jobs have enforced wall-time limits.
- Job artifacts are buffered in host memory rather than streamed.
- Exact/fairness verification, mutation tests, larger protocol worlds and
  additional comparison experiments remain optional research work.

- Live shell drivers currently derive IPv4 addresses from the first remote
  SSH connection and each remote's default route; they expect direct IPv4
  reachability to Linux remotes. Explicit multi-interface/jump-host address
  selection is not implemented.
