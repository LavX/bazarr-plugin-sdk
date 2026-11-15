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


@pytest.mark.parametrize(
    "step", ["leftover_task", "open_generator", "blocking_generator", "stuck_executor"]
)
def test_a_cleanup_step_that_stalls_does_not_hold_the_result(step):
    with host_for(plugins.Stalls) as host:
        response = host.request("translate", {"request": {"step": step}}, wait=LIMIT)
        assert response.value == {"ended": step}


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
