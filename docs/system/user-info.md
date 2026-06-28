# User Info

`user-info` is a built-in resident notification service. Paglets call it when
they need to inform the user about progress, failures, or decisions.

The first implementation prints to the host console. Future implementations can
replace or extend the service to use desktop notifications, logs, email, or
other user-facing channels without changing paglets that call the contract.

## Contract

Import the contract from `paglets.system.user_info`:

```python
from paglets.system.user_info import (
    NOTIFY_USER,
    PI_DONE_USER,
    PI_FAILED_USER,
    PI_OUTPUT_USER,
    PI_PROGRESS_USER,
    STREAM_USER,
    USER_INFO,
    UserInfoRequest,
    UserInfoStreamRequest,
)
```

`UserInfoRequest` contains:

- `severity`, such as `info`, `warning`, or `error`.
- `title`, a short message title.
- `message`, the user-facing detail.
- `source_agent_id`, the paglet that sent the notification.
- `job_id`, when the message belongs to a specific job.
- `timestamp`, optional epoch seconds.
- `metadata`, optional string key/value details.

`UserInfoStreamRequest` contains:

- `stream_id`, an optional job or stream identifier.
- `text`, the raw text chunk to write.
- `target`, either `stdout` or `stderr`.
- `flush`, whether to flush after writing.

Use `STREAM_USER` for generic raw, undecorated output such as generated text
chunks. The Pi example uses `PI_OUTPUT_USER` for raw digit chunks,
`PI_PROGRESS_USER` for compact stderr progress, and `PI_DONE_USER` /
`PI_FAILED_USER` for timestamped completion notifications. Use `NOTIFY_USER` for
other timestamped status or failure messages.

## Usage

```python
from paglets.core.runtime_values import ServiceScope
from paglets.system.user_info import NOTIFY_USER, USER_INFO, UserInfoRequest

service = self.require_contract(USER_INFO, operation=NOTIFY_USER, scope=ServiceScope.MESH)
service.call(
    NOTIFY_USER,
    UserInfoRequest(
        severity="warning",
        title="No suitable host",
        message="No online host can satisfy this job's resource request.",
        source_agent_id=self.agent_id,
        job_id=self.state.job_id,
    ),
)
```

Raw output:

```python
from paglets.core.runtime_values import ServiceScope
from paglets.system.user_info import PI_OUTPUT_USER, USER_INFO, UserInfoStreamRequest

service = self.require_contract(USER_INFO, operation=PI_OUTPUT_USER, scope=ServiceScope.LOCAL)
service.send_oneway(
    PI_OUTPUT_USER,
    UserInfoStreamRequest(stream_id=self.state.job_id, text="14159265", target="stdout"),
    no_delay=True,
)
```

The [Analysis Jobs example](../examples/analysis-jobs.md) uses `user-info`
for unsuitable-host, job-failure, and result-saved messages.
