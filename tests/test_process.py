"""The worker as a real process, started the way the host starts it."""

import os
import time
from pathlib import Path

from bazarr_plugin_sdk import runner
from bazarr_plugin_sdk._result import IN_DATA
from bazarr_plugin_sdk.testing import ProcessHost

ROOT = str(Path(__file__).resolve().parent.parent)
ENTRY = "tests.fixtures.plugins:{}"


def start(name, **options):
    host = ProcessHost(ENTRY.format(name), paths=[ROOT], **options)
    host.wait_ready()
    return host


def test_the_isolated_interpreter_finds_the_sdk_through_the_bootstrap_file():
    with start("Echo") as host:
        assert host.welcome["kind"] == {"kind": "translator", "major": 1, "minor": 0}
        assert host.request("describe").value["backend_types"] == ["echo"]
        assert host.hello["pid"] == host.process.pid


def test_prints_native_writes_and_stdin_reads_cannot_touch_the_frames():
    with start("Noisy") as host:
        response = host.request("translate", {"request": {}})
        assert response.value == {"stdin": "", "ok": True}
        assert host.request("describe").ok
    assert "hello from print" in host.stderr_text
    assert "native write to fd 1" in host.stderr_text


def test_noise_at_import_time_goes_to_stderr_too():
    with start("Echo", env={"PATH": os.environ.get("PATH", ""), "BAZARR_TEST_IMPORT_NOISE": "1"}) as host:
        assert host.request("describe").ok
    assert "noise while importing" in host.stderr_text


def test_the_worker_exits_when_the_host_closes_its_stdin():
    host = start("Echo")
    host.process.stdin.close()
    assert host.process.wait(5) == 0
    host.close()


def test_a_missing_entry_exits_with_the_usage_code():
    host = ProcessHost("tests.fixtures.plugins:Nope", paths=[ROOT])
    assert host.process.wait(10) == 64
    host._stderr_thread.join(2)
    assert "cannot load" in host.stderr_text
    host.close()


def test_a_constructor_that_fails_exits_with_the_start_code_and_a_traceback():
    host = ProcessHost("tests.fixtures.plugins:Exploding", paths=[ROOT])
    assert host.process.wait(10) == 66
    host._stderr_thread.join(2)
    assert "constructor failed" in host.stderr_text
    host.close()


def test_a_request_in_flight_survives_a_noisy_neighbour():
    with start("Slow") as host:
        slow = host.submit("translate", {"request": {"seconds": 0.3}})
        assert host.request("describe").ok
        assert slow.wait().value == {"slept": True}


def test_a_plugin_thread_that_ignores_the_shutdown_cannot_keep_a_finished_worker_alive():
    host = start("Stubborn")
    response = host.submit("translate", {"request": {}})
    deadline = time.monotonic() + 10
    while not response.progress:
        assert time.monotonic() < deadline, "the request never started"
        time.sleep(0.01)
    host.process.stdin.close()
    assert host.process.wait(20) == 0
    host.close()


def test_a_large_list_result_arrives_as_a_list_from_a_real_worker():
    with start("BigShapes") as host:
        response = host.request("translate", {"request": {"shape": "list"}}, wait=60)
    assert response.result == {IN_DATA: True}
    assert isinstance(response.value, list) and len(response.value) == 60000


def test_a_secret_never_reaches_the_worker_stderr_through_a_log_record_or_a_cleanup_exception():
    host = start("LeaksOnCancel")
    try:
        assert host.request("translate", {"request": {"log": True}}, {"key": "sk-process-secret"}).ok
    finally:
        host.close()
    assert "sk-process-secret" not in host.stderr_text
    assert "bad key ***" in host.stderr_text and "using ***" in host.stderr_text


def test_a_cleanup_overrun_in_a_real_worker_answers_inside_the_hosts_grace_then_retires_it():
    # The shipped budget, not a shortened one: the worker is a separate process and the leftover
    # thread never ends, so only the answer's timing and the exit status show what happened.
    host = start("Stalls")
    try:
        begun = time.monotonic()
        response = host.request("translate", {"request": {"step": "stuck_executor"}}, wait=30)
        elapsed = time.monotonic() - begun
        assert response.value == {"ended": "stuck_executor"}  # the result is not lost
        assert elapsed < 5.0  # before the host's cancel_grace would have killed the worker
        assert host.process.wait(20) == runner.EXIT_ABANDONED
        assert host.bye.wait(5)
    finally:
        host.close()
    assert "retiring the worker, left running: shutdown_executor" in host.stderr_text


def test_a_cleanup_step_that_raises_in_a_real_worker_retires_it_too():
    host = start("StepRaises")
    try:
        assert host.request("translate", {"request": {}}, wait=30).ok
        assert host.process.wait(20) == runner.EXIT_ABANDONED
    finally:
        host.close()
    assert "retiring the worker, left running: cancel_remaining_tasks" in host.stderr_text
