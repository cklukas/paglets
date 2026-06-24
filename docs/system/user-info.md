# User Info

`user-info` is a built-in resident notification service. Paglets call it when
they need to inform the user about progress, failures, or decisions.

The first implementation prints to the host console. Future implementations can
replace or extend the service to use desktop notifications, logs, email, or
other user-facing channels without changing paglets that call the contract.

## Contract

Import the contract from `paglets.system.user_info`:

```python
from paglets.system.user_info import NOTIFY_USER, USER_INFO, UserInfoRequest
```

`UserInfoRequest` contains:

- `severity`, such as `info`, `warning`, or `error`.
- `title`, a short message title.
- `message`, the user-facing detail.
- `source_agent_id`, the paglet that sent the notification.
- `job_id`, when the message belongs to a specific job.
- `timestamp`, optional epoch seconds.
- `metadata`, optional string key/value details.

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

The [Analysis Jobs example](../examples/analysis-jobs.md) uses `user-info`
for unsuitable-host, job-failure, and result-saved messages.

