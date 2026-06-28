# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from pathlib import Path

from paglets.artifacts import ArtifactRef
from paglets.core.messages import Message
from paglets.system.compute_slots.groups import (
    GROUP_STATUS_COMPLETE,
    GROUP_STATUS_RETURNING_HOME,
    GROUP_STATUS_WAITING_FOR_HOME,
    CollectingComputeJobPaglet,
    CollectingComputeJobState,
    ResultCollectorPaglet,
    ResultCollectorState,
)


class DemoCollectingJob(CollectingComputeJobPaglet):
    State = CollectingComputeJobState

    def run_compute_job(self) -> None:
        return None


class PayloadCollectingJob(DemoCollectingJob):
    def build_result_payload(self) -> dict[str, int]:
        return {"value": 7}


def test_result_collector_tracks_all_successful_jobs():
    collector = _collector()

    collector.handle_message(Message("register_jobs", {"jobs": [{"job_key": "a"}, {"job_key": "b"}]}))
    collector.handle_message(Message("job_result", {"job_key": "a", "result": {"value": 1}}))
    collector.handle_message(Message("job_result", {"job_key": "b", "result": {"value": 2}}))

    summary = collector.handle_message(Message("summary"))
    assert summary["status"] == GROUP_STATUS_COMPLETE
    assert summary["expected_count"] == 2
    assert summary["completed_count"] == 2
    assert summary["failed_count"] == 0
    assert summary["pending_jobs"] == []


def test_collecting_compute_job_reports_success_to_collector_proxy():
    proxy = _RecordingProxy()
    context = _FakeContext(proxy=proxy)
    job = DemoCollectingJob(
        CollectingComputeJobState(
            group_id="group-0",
            job_key="job-0",
            collector_agent_id="collector",
            collector_host_url="http://home",
            dispose_after_report=False,
        ),
        agent_id="job-agent",
    )
    job._attach(context)

    assert job.report_compute_success({"value": 1}) is True

    assert job.state.report_sent is True
    assert proxy.messages == [
        (
            "job_result",
            {
                "group_id": "group-0",
                "job_key": "job-0",
                "agent_id": "job-agent",
                "host_name": "home",
                "host_url": "http://home",
                "result": {"value": 1},
            },
        )
    ]


def test_collecting_compute_job_after_success_reports_default_payload():
    proxy = _RecordingProxy()
    context = _FakeContext(proxy=proxy)
    job = PayloadCollectingJob(
        CollectingComputeJobState(
            group_id="group-0",
            job_key="job-0",
            collector_agent_id="collector",
            collector_host_url="http://home",
            dispose_after_report=False,
        ),
        agent_id="job-agent",
    )
    job._attach(context)

    job.after_compute_success()

    assert job.state.report_sent is True
    assert proxy.messages == [
        (
            "job_result",
            {
                "group_id": "group-0",
                "job_key": "job-0",
                "agent_id": "job-agent",
                "host_name": "home",
                "host_url": "http://home",
                "result": {"value": 7},
            },
        )
    ]


def test_report_compute_artifact_is_not_followed_by_duplicate_default_report(tmp_path: Path):
    proxy = _RecordingProxy()
    context = _FakeContext(proxy=proxy)
    result_path = tmp_path / "result.db"
    result_path.write_bytes(b"sqlite-bytes")
    job = PayloadCollectingJob(
        CollectingComputeJobState(
            group_id="group-0",
            job_key="job-0",
            collector_agent_id="collector",
            collector_host_url="http://collector",
            dispose_after_report=False,
        ),
        agent_id="job-agent",
    )
    job._attach(context)

    assert job.report_compute_artifact(result_path, result={"bundle": "job-0"}, name="result.db") is True
    job.after_compute_success()

    assert len(proxy.messages) == 1
    kind, payload = proxy.messages[0]
    assert kind == "job_result"
    assert payload["result"]["bundle"] == "job-0"
    assert payload["result"]["artifact"]["artifact_id"] == "artifact-0"
    assert context.uploads == [(result_path, "http://collector", "collector", "result.db")]


def test_after_success_skips_default_report_when_report_was_already_sent():
    proxy = _RecordingProxy()
    context = _FakeContext(proxy=proxy)
    job = PayloadCollectingJob(
        CollectingComputeJobState(
            group_id="group-0",
            job_key="job-0",
            collector_agent_id="collector",
            collector_host_url="http://home",
            dispose_after_report=False,
            report_sent=True,
        ),
        agent_id="job-agent",
    )
    job._attach(context)

    job.after_compute_success()

    assert proxy.messages == []


def test_result_collector_tracks_partial_failure():
    collector = _collector()

    collector.handle_message(Message("register_jobs", {"jobs": [{"job_key": "a"}, {"job_key": "b"}]}))
    collector.handle_message(Message("job_result", {"job_key": "a", "result": {"value": 1}}))
    collector.handle_message(Message("job_failure", {"job_key": "b", "error": "boom"}))

    summary = collector.handle_message(Message("summary"))
    assert summary["status"] == GROUP_STATUS_COMPLETE
    assert summary["completed_count"] == 1
    assert summary["failed_count"] == 1
    assert summary["failures"]["b"]["error"] == "boom"


def test_result_collector_records_duplicate_reports_without_overwriting_original():
    collector = _collector()

    collector.handle_message(Message("register_jobs", {"jobs": [{"job_key": "a"}]}))
    collector.handle_message(Message("job_result", {"job_key": "a", "result": {"value": 1}}))
    collector.handle_message(Message("job_result", {"job_key": "a", "result": {"value": 2}}))

    summary = collector.handle_message(Message("summary"))
    assert summary["results"]["a"]["result"] == {"value": 1}
    assert summary["duplicate_reports"] == [{"job_key": "a", "result": {"value": 2}, "success": True}]


def test_result_collector_summary_reports_completion_without_drain():
    collector = _collector()
    collector.handle_message(Message("register_jobs", {"jobs": [{"job_key": "a"}]}))
    collector.handle_message(Message("job_result", {"job_key": "a", "result": {"value": 1}}))

    summary = collector.handle_message(Message("summary"))

    assert summary["completed_count"] == 1
    assert summary["pending_count"] == 0


def test_result_collector_return_home_waits_when_home_is_offline():
    context = _FakeContext(name="worker", address="http://worker", online=False)
    collector = _collector(context=context, return_home=True)
    deactivations = []
    collector.deactivate = lambda *args, **kwargs: deactivations.append(kwargs)  # type: ignore[method-assign]

    collector.handle_message(Message("register_jobs", {"jobs": [{"job_key": "a"}]}))
    collector.handle_message(Message("job_result", {"job_key": "a", "result": {}}))

    assert collector.state.status == GROUP_STATUS_WAITING_FOR_HOME
    assert deactivations


def test_result_collector_activation_retries_return_home_when_home_is_online():
    context = _FakeContext(name="worker", address="http://worker", online=True)
    collector = _collector(context=context, return_home=True)
    collector.state.status = GROUP_STATUS_WAITING_FOR_HOME
    dispatches = []
    collector.dispatch = lambda target: dispatches.append(target)  # type: ignore[method-assign]

    collector.run()

    assert collector.state.status == GROUP_STATUS_RETURNING_HOME
    assert dispatches == ["http://home"]


def _collector(*, context=None, return_home: bool = False) -> ResultCollectorPaglet:
    state = ResultCollectorState(
        group_id="group-0",
        home_host_name="home",
        home_host_url="http://home",
        return_home_when_complete=return_home,
        home_check_seconds=1.0,
    )
    collector = ResultCollectorPaglet(state)
    collector._attach(context or _FakeContext())
    return collector


class _FakeContext:
    def __init__(self, *, name: str = "home", address: str = "http://home", online: bool = True, proxy=None):
        self.name = name
        self.address = address
        self._online = online
        self._proxy = proxy
        self.uploads = []

    def is_host_online(self, name_or_url: str) -> bool:
        return self._online

    def get_proxy(self, agent_id: str, host_url: str | None = None):
        return self._proxy

    def upload_artifact(
        self,
        path,
        *,
        host_url: str | None = None,
        owner_agent_id: str = "",
        name: str | None = None,
    ) -> ArtifactRef:
        source = Path(path)
        self.uploads.append((source, host_url, owner_agent_id, name))
        return ArtifactRef(
            host_url=(host_url or self.address).rstrip("/"),
            artifact_id=f"artifact-{len(self.uploads) - 1}",
            name=name or source.name,
            size_bytes=source.stat().st_size,
            owner_agent_id=owner_agent_id,
        )


class _RecordingProxy:
    def __init__(self):
        self.messages = []

    def send(self, message: Message, **kwargs):
        self.messages.append((message.kind, dict(message.args)))
        return {"ok": True}
