"""Every step after a request ends or is cancelled is bounded, and the host still gets its reply.

Each test stalls the worker in one step and checks that the reply arrives while the stall is still
going on. The shared budget is cut to a fraction of a second; the limit the tests wait for is only
what a hang would hit.
"""

import asyncio
import threading
import time

import pytest

from bazarr_plugin_sdk import runner
from bazarr_plugin_sdk._grace import Grace
from bazarr_plugin_sdk.testing import LoopbackHost
from tests.fixtures import plugins

BUDGET = 0.5
# The shipped values, before the fixture below shortens the budget for the other tests.
SHIPPED_BUDGET = runner.SHUTDOWN_TIMEOUT + runner.CLEANUP_SLACK
LIMIT = 20


@pytest.fixture(autouse=True)
def short_budget(monkeypatch):
    monkeypatch.setattr(runner, "SHUTDOWN_TIMEOUT", BUDGET)
    plugins.RELEASE.clear()
    yield
    plugins.RELEASE.set()  # lets every stalled thread go


def started(host, step, plugin=None):
    response = host.submit("translate", {"request": {"step": step}})
    deadline = time.monotonic() + LIMIT
    while not response.progress and not response.done.is_set():
        assert time.monotonic() < deadline, "the request never started"
        time.sleep(0.01)
    return response


def host_for(plugin_class, **options):
    host = LoopbackHost(plugin_class, **options)
    host.wait_ready()
    return host


def wait_for_exit(host):
    deadline = time.monotonic() + LIMIT
    while host.exit_code is None:
        assert time.monotonic() < deadline, "the worker never exited"
        time.sleep(0.01)
    return host.exit_code


@pytest.mark.parametrize("plugin, step", [(plugins.Stalls, "swallows_cancel"), (plugins.Stalls, "blocks_the_loop"), (plugins.StallsSync, "x")])
def test_a_worker_answers_a_call_that_ignored_its_cancel_then_exits_and_runs_nothing_beside_it(plugin, step):
    plugins.StallsSync.started.clear()
    host = host_for(plugin)
    response = started(host, step)
    host.cancel(response)
    assert response.wait(LIMIT).code == "cancelled"
    assert wait_for_exit(host) == runner.EXIT_ABANDONED
    assert host.bye.wait(LIMIT)
    # A call made now is refused or finds no worker; it never runs beside the stuck thread.
    try:
        late = host.submit("translate", {"request": {"step": "leftover_task"}})
        assert late.wait(LIMIT).code in {"shutting_down", "worker_exited"}
    except (OSError, ValueError):
        pass
    assert len(plugins.StallsSync.started) <= 1
    host.close()


def test_a_request_that_stops_in_time_leaves_the_worker_running():
    with host_for(plugins.Stalls) as host:
        assert host.request("translate", {"request": {"step": "clean"}}, wait=LIMIT).ok
        assert host.request("describe", wait=LIMIT).ok
        assert host.exit_code is None


@pytest.mark.parametrize("step", ["swallows_cancel", "blocks_the_loop"])
def test_a_request_that_ignores_its_cancel_is_answered_as_cancelled(step):
    with host_for(plugins.Stalls) as host:
        response = started(host, step)
        host.cancel(response)
        assert response.wait(LIMIT).code == "cancelled"


def test_a_plain_method_that_ignores_its_cancel_is_answered_as_cancelled():
    with host_for(plugins.StallsSync) as host:
        response = started(host, "x")
        host.cancel(response)
        assert response.wait(LIMIT).code == "cancelled"


def assert_retired(host):
    """The worker has answered, said goodbye, ended with the retire status and refuses new calls."""
    assert wait_for_exit(host) == runner.EXIT_ABANDONED
    assert host.bye.wait(LIMIT)
    try:
        late = host.submit("describe")
        assert late.wait(LIMIT).code in {"shutting_down", "worker_exited"}
    except (OSError, ValueError):
        pass  # the pipe is already closed: nothing can run


@pytest.mark.parametrize(
    "step", ["leftover_task", "open_generator", "blocking_generator", "stuck_executor", "to_thread"]
)
def test_a_cleanup_step_that_stalls_still_answers_and_then_retires_the_worker(step):
    host = host_for(plugins.Stalls)
    try:
        response = host.request("translate", {"request": {"step": step}}, wait=LIMIT)
        assert response.value == {"ended": step}  # the result is not lost
        assert_retired(host)
    finally:
        host.close()


def test_a_settle_call_the_host_never_answers_does_not_hold_a_cancelled_request():
    with host_for(plugins.Stalls, host_calls={"usage": lambda params: plugins.RELEASE.wait(120) and {}}) as host:
        response = started(host, "settle_after_cancel")
        host.cancel(response)
        assert response.wait(LIMIT).done.is_set()


def test_a_loop_that_will_not_close_does_not_hold_the_result(monkeypatch):
    real_close = asyncio.BaseEventLoop.close
    real_new = asyncio.new_event_loop
    mine = []

    def new_event_loop():
        loop = real_new()
        mine.append(loop)
        return loop

    def close(self):
        # Only the loops this test's requests make. A loop an earlier test left behind is
        # closed by the garbage collector, on whatever thread happens to be running.
        if any(self is loop for loop in mine):
            plugins.RELEASE.wait(120)
        return real_close(self)

    monkeypatch.setattr(asyncio, "new_event_loop", new_event_loop)
    monkeypatch.setattr(asyncio.BaseEventLoop, "close", close)
    with host_for(plugins.Echo) as host:
        assert host.request("describe", wait=LIMIT).ok
        assert host.request("translate", {"request": {}}, wait=LIMIT).ok
    assert mine  # the translate request did run on a loop of its own


def test_the_steps_share_one_budget_and_do_not_each_get_their_own(monkeypatch):
    monkeypatch.setattr(runner, "SHUTDOWN_TIMEOUT", 1.0)
    with host_for(plugins.Stalls) as host:
        begun = time.monotonic()
        response = host.request("translate", {"request": {"step": "all_three"}}, wait=LIMIT)
        elapsed = time.monotonic() - begun
    assert response.value == {"ended": "all_three"}
    # three stalled steps at a full second each would take three seconds; one budget takes about one
    assert elapsed < 2.4


def test_a_stream_that_will_not_flush_does_not_hold_the_exit():
    release = threading.Event()

    class Stuck:
        def flush(self):
            release.wait(120)

    grace = Grace(BUDGET)
    grace.start()
    try:
        begun = time.monotonic()
        runner._flush(Stuck(), grace)
        assert time.monotonic() - begun < 10
    finally:
        release.set()


def test_the_grace_is_one_deadline_that_starts_once(monkeypatch):
    now = [100.0]
    monkeypatch.setattr("bazarr_plugin_sdk._grace.time.monotonic", lambda: now[0])
    grace = Grace(5.0)
    assert grace.remaining() == 5.0 and not grace.expired()
    now[0] = 103.0
    assert grace.remaining() == 5.0  # not started: nothing has been spent
    grace.start()
    now[0] = 105.0
    grace.start()  # a second start changes nothing
    assert grace.remaining() == 3.0 and not grace.expired()
    now[0] = 108.5
    assert grace.remaining() == 0.0 and grace.expired()


def test_the_channel_lock_is_reentrant_for_the_filter_and_the_loop_handler():
    from bazarr_plugin_sdk._scrub import held_secrets

    host = host_for(plugins.Echo)
    channel = host.worker.channel
    try:
        # A finaliser may run the filter or the loop handler on a thread that holds the lock.
        assert channel._lock.acquire(timeout=5)
        try:
            again = channel._lock.acquire(timeout=1)
            if again:
                channel._lock.release()
            assert again, "the channel lock cannot be taken again by the thread that holds it"
            assert held_secrets(channel) == []
        finally:
            channel._lock.release()
    finally:
        host.close()


def test_the_whole_cleanup_budget_is_well_under_the_hosts_cancel_grace():
    # The host kills a worker that has not answered a cancel within its cancel_grace (5 seconds in
    # provider_hub/runtime/session.py). The reply comes at most this long after a cancel.
    assert SHIPPED_BUDGET <= 4.0
