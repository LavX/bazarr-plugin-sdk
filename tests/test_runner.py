import json
import time

import pytest

from bazarr_plugin_sdk import ERROR_CODES
from bazarr_plugin_sdk.runner import INLINE_RESULT_BYTES, build_plugin, pack_result
from bazarr_plugin_sdk._protocol.worker_channel import OperationResult
from bazarr_plugin_sdk.errors import PluginError
from bazarr_plugin_sdk.testing import HostFailure, LoopbackHost
from tests.fixtures import plugins


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
    assert response.result == {"value": [{"value": "f", "secret_seen": "k1"}]}
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
    assert large.result == {"json_in_data": True} and large.data
    assert len(large.value["lines"]) == 1500


def test_pack_result_switches_at_the_inline_limit():
    assert pack_result({"a": 1}) == {"a": 1}
    assert pack_result([1]) == {"value": [1]}
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
        time.sleep(0.4)
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
        assert response.wait(3).code == "cancelled"
        time.sleep(0.2)
        assert [name for _, name, _ in host.calls] == ["reserve"]
        assert host.request("describe").ok


def test_cancel_sets_the_event_for_code_that_watches_it():
    with host_for(plugins.WatchesEvent) as host:
        response = host.submit("translate", {"request": {}})
        time.sleep(0.2)
        host.cancel(response)
        assert response.wait(3).code == "cancelled"


def test_cancel_reaches_a_blocking_request_through_the_flag():
    with host_for(plugins.Blocking) as host:
        response = host.submit("translate", {"request": {}})
        time.sleep(0.1)
        host.cancel(response)
        assert response.wait(3).code == "cancelled"


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
        a = host.submit("translate", {"request": {"seconds": 0.5}})
        b = host.submit("translate", {"request": {"seconds": 0.5}})
        c = host.submit("translate", {"request": {"seconds": 0.0}})
        begun = time.monotonic()
        assert c.wait().code == "busy"
        assert a.wait().ok and b.wait().ok
        assert time.monotonic() - begun < 0.9


def test_the_worker_heartbeats_on_its_own_while_idle():
    with host_for(plugins.Echo, heartbeat_interval=0.05) as host:
        time.sleep(0.5)
        assert len(host.heartbeats) >= 3
        assert host.heartbeats[0]["in_flight"] == 0


def test_shutdown_cancels_running_calls_and_says_bye():
    host = host_for(plugins.WatchesEvent)
    response = host.submit("translate", {"request": {}})
    time.sleep(0.2)
    host.shutdown()
    assert response.wait(5).code == "cancelled"
    assert host.bye.wait(5)
    assert host.close() == 0


def test_the_worker_ends_when_the_host_closes_the_pipe():
    host = host_for(plugins.Echo)
    host._close_file(1)
    deadline = time.monotonic() + 3
    while host.exit_code is None and time.monotonic() < deadline:
        time.sleep(0.05)
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
