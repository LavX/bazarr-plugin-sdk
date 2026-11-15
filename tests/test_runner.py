import json
import time

import pytest

from bazarr_plugin_sdk import ERROR_CODES
from bazarr_plugin_sdk.runner import INLINE_RESULT_BYTES, build_plugin, pack_result
from bazarr_plugin_sdk._protocol.worker_channel import OperationResult
from bazarr_plugin_sdk._result import IN_DATA, WRAPPED
from bazarr_plugin_sdk.errors import PluginError
from bazarr_plugin_sdk.testing import HostFailure, LoopbackHost
from tests.fixtures import plugins


def wait_until(condition, timeout=10.0):
    """Poll until ``condition()`` holds. A limit that only a hang can reach, not a timing margin."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "the condition never held"
        time.sleep(0.01)


def host_for(plugin_class, **options):
    host = LoopbackHost(plugin_class, **options)
    host.wait_ready()
    return host


def test_the_hello_offers_the_kind_and_the_plugin_limit():
    with host_for(plugins.Echo) as host:
        hello = host.hello
    assert hello["kinds"] == [{"kind": "translator", "major": 1, "minor": 0}]
    assert hello["protocols"] == [{"major": 2, "minor": 0}]
    assert hello["max_in_flight"] == 4
    assert hello["worker"]["name"].endswith("fixtures.plugins.Echo")


def test_a_host_that_wants_another_kind_major_rejects_the_worker():
    host = LoopbackHost(plugins.Echo, kind_versions=((2, 0),))
    with pytest.raises(HostFailure) as caught:
        host.wait_ready()
    assert caught.value.code == "handshake_failed" and "version mismatch" in str(caught.value)
    assert host.close() == 3


def test_a_class_of_no_known_kind_is_refused():
    with pytest.raises(PluginError) as caught:
        build_plugin(plugins.Broken)
    assert caught.value.reason == "entry"


def test_operations_and_their_arguments():
    with host_for(plugins.Echo) as host:
        assert host.request("validate", {"config": {"a": 1}}).value == {"normalized": True, "a": 1}
        assert host.request("migrate", {"from_version": 1, "settings": {"s": 1}, "connections": []}).value == {
            "settings": {"s": 1, "v": 2}, "connections": [],
        }
        error = host.request("validate", {"config": {"bad": 1}}).error
        assert error["code"] == "invalid_config" and error["field_errors"] == {"bad": "no"}
        assert error["message"] == "bad setting" and error["retryable"] is False


def test_a_list_result_travels_as_a_value():
    with host_for(plugins.Echo) as host:
        response = host.request("options", {"field": "f", "config": {}}, {"key": "k1"})
    assert response.result == {WRAPPED: [{"value": "f", "secret_seen": "k1"}]}
    assert response.value == [{"value": "f", "secret_seen": "k1"}]


def test_secrets_reach_options_and_test_as_an_argument_and_translate_inside_the_request():
    with host_for(plugins.Echo) as host:
        assert host.request("test", {"config": {}}, {"key": "k2"}).value["key"] == "k2"
        assert host.request("translate", {"request": {"lines": []}}, {"key": "k3"}).value["secret_seen"] == "k3"
        assert host.request("translate", {"request": {}}).value["secret_seen"] is None


def test_unknown_operations_and_wrong_arguments_are_errors_not_crashes():
    with host_for(plugins.Echo) as host:
        assert host.request("nope").code == "unknown_op"
        assert host.request("validate", {"wrong": 1}).code == "invalid_request"
        assert host.request("translate", {"request": "not an object"}).code == "invalid_request"
        assert host.request("validate", {"config": {}}, {"s": "x"}).ok  # secrets are ignored where not taken
        assert host.request("describe").ok


def test_plugin_errors_keep_their_parts():
    with host_for(plugins.Failing) as host:
        rate = host.request("translate", {"request": {"kind": "rate"}}).error
        assert (rate["code"], rate["retry_after"], rate["retryable"]) == ("rate_limited", 12.0, True)
        declined = host.request("translate", {"request": {"kind": "declined"}}).error
        assert (declined["code"], declined["reason"]) == ("declined", "language_unsupported")


def test_an_unhandled_exception_is_a_plugin_error_with_a_traceback_and_the_worker_lives():
    with host_for(plugins.Failing) as host:
        error = host.request("translate", {"request": {}}).error
        assert error["code"] == "plugin_error" and error["class_name"] == "RuntimeError"
        assert "boom" in error["traceback"]
        assert host.request("describe").ok


def test_a_small_result_is_inline_and_a_large_one_travels_as_data_frames():
    with host_for(plugins.BigResult) as host:
        small = host.request("translate", {"request": {"count": 3}})
        assert small.data == b"" and len(small.value["lines"]) == 3
        large = host.request("translate", {"request": {"count": 1500}})
    assert large.result == {IN_DATA: True} and large.data
    assert len(large.value["lines"]) == 1500


def test_a_large_list_or_scalar_result_comes_back_as_it_is():
    with host_for(plugins.BigShapes) as host:
        as_list = host.request("translate", {"request": {"shape": "list"}}, wait=30)
        as_text = host.request("translate", {"request": {"shape": "text"}}, wait=30)
        as_dict = host.request("translate", {"request": {"shape": "dict"}}, wait=30)
    for response in (as_list, as_text, as_dict):
        assert response.result == {IN_DATA: True} and response.data
    assert isinstance(as_list.value, list) and len(as_list.value) == 60000 and as_list.value[1] == "line 1" * 5
    assert as_text.value == "x" * 700_000
    assert set(as_dict.value) == {"value", "other"}


def test_the_data_of_a_large_non_object_result_is_the_json_of_the_result_itself():
    packed = pack_result(["z" * INLINE_RESULT_BYTES])
    assert isinstance(packed, OperationResult) and json.loads(packed.data) == ["z" * INLINE_RESULT_BYTES]
    assert json.loads(pack_result("z" * INLINE_RESULT_BYTES).data) == "z" * INLINE_RESULT_BYTES


def test_pack_result_switches_at_the_inline_limit():
    assert pack_result({"a": 1}) == {"a": 1}
    assert pack_result([1]) == {WRAPPED: [1]}
    packed = pack_result({"x": "y" * INLINE_RESULT_BYTES})
    assert isinstance(packed, OperationResult) and json.loads(packed.data)["x"].startswith("y")


def test_a_result_over_the_limit_the_request_names_is_an_error():
    with host_for(plugins.BigResult) as host:
        response = host.request("translate", {"request": {"count": 1500}}, max_result_bytes=1000)
        assert response.code == "result_too_large"
        assert host.request("describe").ok


def test_a_result_that_is_not_json_is_an_error():
    with host_for(plugins.NotJson) as host:
        error = host.request("translate", {"request": {}}).error
    assert error["code"] == "internal" and error["reason"] == "result_not_json"


def test_reserve_usage_and_checkpoint_are_host_calls_that_wait_for_the_ack():
    with host_for(plugins.Spender) as host:
        response = host.request("translate", {"request": {}})
    assert response.value == {"hold": "hold-1"}
    assert [name for _, name, _ in host.calls] == ["reserve", "usage", "checkpoint"]
    assert host.calls[0][2] == {"amount": 0.5, "model": "m1"}
    assert host.calls[1][2] == {"tokens": 10, "cost": 0.4, "model": "m1", "reservations": ["hold-1"]}
    done = [p["data"]["done"] for p in response.progress if p.get("data")]
    assert done[0] == 0 and done[-1] == 2  # the middle report may be dropped by the throttle


def test_nothing_is_sent_after_a_reserve_until_it_is_acknowledged():
    import threading

    gate = threading.Event()

    def slow_reserve(params):
        gate.wait(5)
        return {"hold": "h"}

    with host_for(plugins.Spender, host_calls={"reserve": slow_reserve}) as host:
        response = host.submit("translate", {"request": {}})
        wait_until(lambda: [name for _, name, _ in host.calls] == ["reserve"] and response.progress)
        # progress "start" and the reserve call only: the "sent" report waits for the answer
        assert [p["data"]["done"] for p in response.progress if p.get("data")] == [0]
        assert [name for _, name, _ in host.calls] == ["reserve"]
        gate.set()
        assert response.wait().value == {"hold": "h"}


def test_a_refused_record_raises_in_the_plugin():
    def refuse(params):
        raise HostFailure("month spent", code="quota_exhausted")

    with host_for(plugins.Spender, host_calls={"reserve": refuse}) as host:
        error = host.request("translate", {"request": {}}).error
    assert (error["code"], error["message"]) == ("quota_exhausted", "month spent")


def test_a_host_without_the_call_answers_unknown_call():
    with host_for(plugins.Spender, host_calls={"reserve": None}) as host:
        host.host_calls.pop("reserve")
        error = host.request("translate", {"request": {}}).error
    assert error["reason"] == "unknown_call"


def test_cancel_stops_an_async_request_and_it_starts_no_new_spending():
    with host_for(plugins.Hanging) as host:
        response = host.submit("translate", {"request": {}})
        deadline = time.monotonic() + 2
        while not host.calls and time.monotonic() < deadline:
            time.sleep(0.01)
        host.cancel(response)
        assert response.wait(10).code == "cancelled"
        assert host.request("describe").ok  # answered after anything the cancelled request might still send
        assert [name for _, name, _ in host.calls] == ["reserve"]


def test_cancel_sets_the_event_for_code_that_watches_it():
    with host_for(plugins.WatchesEvent) as host:
        response = host.submit("translate", {"request": {}})
        wait_until(lambda: response.progress)
        host.cancel(response)
        assert response.wait(10).code == "cancelled"


def test_cancel_reaches_a_blocking_request_through_the_flag():
    with host_for(plugins.Blocking) as host:
        response = host.submit("translate", {"request": {}})
        wait_until(lambda: response.progress)
        host.cancel(response)
        assert response.wait(10).code == "cancelled"


def test_a_blocking_request_can_use_the_host_calls_from_its_thread():
    with host_for(plugins.ThreadHolds) as host:
        response = host.request("translate", {"request": {}})
    assert response.value["hold"] == "hold-1"


def test_the_budget_param_reaches_the_context_and_is_not_an_argument():
    with host_for(plugins.Budgeted) as host:
        assert host.request("test", {"config": {}, "budget": 0.5}, {}).value == {"budget": 0.5}
        assert host.request("test", {"config": {}}, {}).value == {"budget": None}


def test_calls_run_concurrently_up_to_the_limit_and_the_worker_refuses_more():
    with host_for(plugins.Slow) as host:
        a = host.submit("translate", {"request": {"seconds": 60}})
        b = host.submit("translate", {"request": {"seconds": 60}})
        # The worker takes a request's slot before it reads the next frame, so a and b hold both.
        c = host.submit("translate", {"request": {"seconds": 0.0}})
        assert c.wait(10).code == "busy"
        assert not a.done.is_set() and not b.done.is_set()
        host.cancel(a)
        host.cancel(b)
        assert a.wait(10).code == "cancelled" and b.wait(10).code == "cancelled"


def test_the_worker_heartbeats_on_its_own_while_idle():
    with host_for(plugins.Echo, heartbeat_interval=0.05) as host:
        wait_until(lambda: len(host.heartbeats) >= 3)
        assert host.heartbeats[0]["in_flight"] == 0


def test_shutdown_cancels_running_calls_and_says_bye():
    host = host_for(plugins.WatchesEvent)
    response = host.submit("translate", {"request": {}})
    wait_until(lambda: response.progress)
    host.shutdown()
    assert response.wait(10).code == "cancelled"
    assert host.bye.wait(10)
    assert host.close() == 0


def test_the_worker_ends_when_the_host_closes_the_pipe():
    host = host_for(plugins.Echo)
    host._close_file(1)
    wait_until(lambda: host.exit_code is not None)
    assert host.exit_code == 0
    host.close()


def test_error_codes_a_plugin_raises_are_all_in_the_vocabulary():
    assert {"rate_limited", "declined", "invalid_config"} <= ERROR_CODES


def test_secret_values_are_scrubbed_from_a_plugin_error_and_an_unhandled_one():
    with host_for(plugins.Leaky) as host:
        planned = host.request("translate", {"request": {"kind": "plugin"}}, {"key": "sk-very-secret"}).error
        crash = host.request("translate", {"request": {"kind": "crash"}}, {"key": "sk-very-secret"}).error
    assert planned["code"] == "auth_required" and "sk-very-secret" not in str(planned) and "***" in planned["message"]
    assert "sk-very-secret" not in str(crash) and "***" in crash["traceback"]


def test_a_plugin_error_is_scrubbed_of_a_secret_an_earlier_request_carried():
    class Remembers(plugins.Leaky):
        kept = ""

        async def translate(self, request, ctx):
            if request["kind"] == "keep":
                type(self).kept = request["secrets"]["key"]
                return {}
            raise PluginError("auth_required", f"the key {type(self).kept} was refused")

    with host_for(Remembers) as host:
        assert host.request("translate", {"request": {"kind": "keep"}}, {"key": "sk-earlier-secret"}).ok
        error = host.request("translate", {"request": {"kind": "plugin"}}).error
    assert error["code"] == "auth_required" and "sk-earlier-secret" not in str(error) and "***" in error["message"]


def test_a_secret_in_a_field_name_does_not_leave_the_worker():
    class Names(plugins.Echo):
        async def translate(self, request, ctx):
            key = request["secrets"]["key"]
            raise PluginError("invalid_config", "bad", field_errors={f"field {key}": "wrong"})

    with host_for(Names) as host:
        error = host.request("translate", {"request": {}}, {"key": "sk-field-secret"}).error
    assert "sk-field-secret" not in str(error) and error["field_errors"] == {"field ***": "wrong"}


def test_a_worker_with_room_for_one_request_never_answers_busy_to_a_back_to_back_host():
    class One(plugins.Echo):
        max_in_flight = 1

    with host_for(One) as host:
        for _ in range(200):
            assert host.request("describe").ok
        for _ in range(50):
            assert host.request("validate", {"config": {"bad": 1}}).code == "invalid_config"


def test_a_task_a_request_leaves_running_is_cancelled_and_allowed_to_clean_up():
    import asyncio
    import threading

    from bazarr_plugin_sdk.runner import _run_coroutine

    cleaned = threading.Event()

    async def background():
        try:
            await asyncio.sleep(60)
        finally:
            cleaned.set()

    async def request():
        asyncio.get_running_loop().create_task(background())
        await asyncio.sleep(0)
        return "done"

    class Operation:
        cancelled = threading.Event()
        cancel_reason = None

    assert _run_coroutine(Operation(), None, request()) == "done"
    assert cleaned.is_set()


def test_a_thread_that_will_not_finish_cannot_hold_the_loop_shutdown(monkeypatch):
    import asyncio
    import threading

    from bazarr_plugin_sdk import runner

    monkeypatch.setattr(runner, "SHUTDOWN_TIMEOUT", 0.3)
    release = threading.Event()

    class Operation:
        cancelled = threading.Event()
        cancel_reason = None

    async def request():
        asyncio.get_running_loop().run_in_executor(None, release.wait, 60)
        await asyncio.sleep(0.05)
        return "done"

    try:
        started = time.monotonic()
        assert runner._run_coroutine(Operation(), None, request()) == "done"
        assert time.monotonic() - started < 30  # not the 60 seconds the thread would take
    finally:
        release.set()


def _start_runner(*args):
    import subprocess
    import sys
    from pathlib import Path

    import bazarr_plugin_sdk

    bootstrap = Path(bazarr_plugin_sdk.__file__).parent / "_bootstrap.py"
    return subprocess.run([sys.executable, "-I", str(bootstrap), *args], capture_output=True, timeout=60)


def test_a_bad_command_line_exits_64_and_writes_nothing_to_the_frame_pipe():
    result = _start_runner("--bogus")
    assert result.returncode == 64 and result.stdout == b"" and b"usage" in result.stderr


def test_help_goes_to_stderr_not_the_frame_pipe():
    result = _start_runner("--help")
    assert result.returncode == 0 and result.stdout == b"" and b"--entry" in result.stderr


def test_the_executor_shutdown_is_bounded_where_the_loop_cannot_bound_it(monkeypatch):
    """Python 3.10 and 3.11 join the executor threads without a limit, even when cancelled."""
    import asyncio
    import threading

    from bazarr_plugin_sdk import runner

    async def shutdown_without_a_limit(self):
        self._executor_shutdown_called = True
        executor = self._default_executor
        if executor is None:
            return
        future = self.create_future()

        def work():
            try:
                executor.shutdown(wait=True)
                self.call_soon_threadsafe(future.set_result, None)
            except Exception as error:
                self.call_soon_threadsafe(future.set_exception, error)

        thread = threading.Thread(target=work)
        thread.start()
        try:
            await future
        finally:
            thread.join()

    monkeypatch.setattr(asyncio.BaseEventLoop, "shutdown_default_executor", shutdown_without_a_limit)
    monkeypatch.setattr(runner, "SHUTDOWN_TIMEOUT", 0.3)
    release = threading.Event()

    class Operation:
        cancelled = threading.Event()
        cancel_reason = None

    async def request():
        asyncio.get_running_loop().run_in_executor(None, release.wait, 60)
        await asyncio.sleep(0.05)
        return "done"

    result = []
    runner_thread = threading.Thread(target=lambda: result.append(runner._run_coroutine(Operation(), None, request())), daemon=True)
    try:
        runner_thread.start()
        runner_thread.join(30)
        assert not runner_thread.is_alive(), "the request was held by a stuck executor thread"
        assert result == ["done"]
    finally:
        release.set()


def test_a_task_that_swallows_its_cancel_cannot_hold_the_request(monkeypatch, caplog):
    import asyncio
    import threading

    from bazarr_plugin_sdk import runner

    monkeypatch.setattr(runner, "SHUTDOWN_TIMEOUT", 0.3)
    stubborn_ran = threading.Event()

    async def stubborn():
        while True:
            try:
                stubborn_ran.set()
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                pass  # swallows every cancel

    async def request():
        asyncio.get_running_loop().create_task(stubborn())
        await asyncio.sleep(0.05)
        return "done"

    class Operation:
        cancelled = threading.Event()
        cancel_reason = None

    result = []
    worker = threading.Thread(target=lambda: result.append(runner._run_coroutine(Operation(), None, request())), daemon=True)
    with caplog.at_level("WARNING", logger="bazarr_plugin_sdk.runner"):
        worker.start()
        worker.join(30)
    assert not worker.is_alive(), "a task that swallows its cancel held the request"
    assert result == ["done"] and stubborn_ran.is_set()
    assert "ignored its cancel" in caplog.text


@pytest.mark.parametrize(
    "shape, expected",
    [
        ("value", {"value": 1}),
        ("marker", {"json_in_data": True, "x": 1}),
        ("both", {"value": 1, "json_in_data": True}),
        ("empty", {}),
    ],
)
def test_an_ordinary_result_object_is_never_unwrapped(shape, expected):
    with host_for(plugins.Shapes) as host:
        response = host.request("translate", {"request": {"shape": shape}})
    assert response.result == expected and response.value == expected


def test_a_large_object_with_a_value_field_comes_back_whole():
    with host_for(plugins.Shapes) as host:
        response = host.request("translate", {"request": {"shape": "large_value"}}, wait=30)
    assert response.result == {IN_DATA: True}
    assert response.value == {"value": "y" * 700_000}


@pytest.mark.parametrize("key", [WRAPPED, IN_DATA])
def test_a_result_that_uses_a_reserved_key_is_refused(key):
    with host_for(plugins.Shapes) as host:
        error = host.request("translate", {"request": {"shape": "reserved", "key": key}}).error
    assert error["code"] == "internal" and error["reason"] == "reserved_key"


def test_a_secret_in_an_exception_a_background_task_raises_on_cancel_is_not_logged(caplog):
    with caplog.at_level("DEBUG"):
        with host_for(plugins.LeaksOnCancel) as host:
            response = host.request("translate", {"request": {}}, {"key": "sk-bg-secret"})
    assert response.ok
    assert "sk-bg-secret" not in caplog.text
    assert "bad key ***" in caplog.text  # it was logged, cleaned
