import errno
import os
import shutil

import pytest

from bazarr_plugin_sdk import fs


@pytest.fixture
def sandboxed(monkeypatch):
    """The metadata calls a worker's sandbox refuses, as they fail there."""

    def refuse(*args, **kwargs):
        raise PermissionError(errno.EPERM, "Operation not permitted")

    for name in ("chmod", "chown", "lchown", "lchmod", "fchmod", "fchown", "utime", "setxattr", "removexattr", "listxattr", "getxattr"):
        if hasattr(os, name):
            monkeypatch.setattr(os, name, refuse)
    for name in ("copystat", "copymode", "_copyxattr"):
        monkeypatch.setattr(shutil, name, refuse, raising=False)
    return refuse


def test_the_fixture_makes_the_standard_calls_fail(tmp_path, sandboxed):
    source = tmp_path / "a.txt"
    source.write_text("data")
    for func in (shutil.copy, shutil.copy2):
        with pytest.raises(PermissionError):
            func(source, tmp_path / "b.txt")
    with pytest.raises((PermissionError, shutil.Error)):
        shutil.copytree(tmp_path, tmp_path / "tree")


def test_copy_file_copies_the_bytes_and_replaces_the_target(tmp_path, sandboxed):
    source = tmp_path / "a.bin"
    source.write_bytes(b"\x00\x01" * 1000)
    target = tmp_path / "b.bin"
    target.write_bytes(b"old")
    assert fs.copy_file(source, target) == target
    assert target.read_bytes() == source.read_bytes()
    assert sorted(p.name for p in tmp_path.iterdir()) == ["a.bin", "b.bin"]  # no temporary left behind


def test_a_failed_copy_leaves_no_partial_file(tmp_path, sandboxed):
    with pytest.raises(FileNotFoundError):
        fs.copy_file(tmp_path / "missing", tmp_path / "target")
    assert list(tmp_path.iterdir()) == []


def test_write_file_is_atomic_and_takes_text_or_bytes(tmp_path, sandboxed):
    assert fs.write_file(tmp_path / "t.txt", "café").read_bytes() == "café".encode()
    assert fs.write_file(tmp_path / "b.bin", b"\xff").read_bytes() == b"\xff"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["b.bin", "t.txt"]


def test_touch_creates_and_leaves_an_existing_file_alone(tmp_path, sandboxed):
    path = tmp_path / "flag"
    fs.touch(path)
    assert path.exists() and path.read_bytes() == b""
    path.write_bytes(b"keep")
    fs.touch(path)
    assert path.read_bytes() == b"keep"


def test_copy_tree_copies_files_directories_and_links(tmp_path, sandboxed):
    source = tmp_path / "src"
    (source / "sub" / "deep").mkdir(parents=True)
    (source / "one.txt").write_text("1")
    (source / "sub" / "deep" / "two.txt").write_text("2")
    os.symlink("one.txt", source / "link")
    result = fs.copy_tree(source, tmp_path / "dst")
    assert result == tmp_path / "dst"
    assert (tmp_path / "dst" / "one.txt").read_text() == "1"
    assert (tmp_path / "dst" / "sub" / "deep" / "two.txt").read_text() == "2"
    assert os.readlink(tmp_path / "dst" / "link") == "one.txt"


def test_copy_tree_can_follow_links(tmp_path, sandboxed):
    source = tmp_path / "src"
    source.mkdir()
    (source / "one.txt").write_text("1")
    os.symlink("one.txt", source / "link")
    fs.copy_tree(source, tmp_path / "dst", symlinks=False)
    assert not (tmp_path / "dst" / "link").is_symlink() and (tmp_path / "dst" / "link").read_text() == "1"


def test_copy_tree_refuses_an_existing_target_unless_told_otherwise(tmp_path, sandboxed):
    source = tmp_path / "src"
    source.mkdir()
    (source / "a").write_text("new")
    target = tmp_path / "dst"
    target.mkdir()
    (target / "a").write_text("old")
    with pytest.raises(FileExistsError):
        fs.copy_tree(source, target)
    fs.copy_tree(source, target, dirs_exist_ok=True)
    assert (target / "a").read_text() == "new"


def test_copy_tree_needs_a_directory(tmp_path):
    (tmp_path / "f").write_text("x")
    with pytest.raises(NotADirectoryError):
        fs.copy_tree(tmp_path / "f", tmp_path / "g")


def test_move_renames_within_a_filesystem(tmp_path, sandboxed):
    (tmp_path / "a").write_text("x")
    assert fs.move(tmp_path / "a", tmp_path / "b") == tmp_path / "b"
    assert not (tmp_path / "a").exists() and (tmp_path / "b").read_text() == "x"


def test_move_copies_and_removes_when_the_rename_crosses_filesystems(tmp_path, sandboxed, monkeypatch):
    real_replace = os.replace
    calls = []

    def replace(src, dst):
        calls.append((str(src), str(dst)))
        if str(src).endswith(".part"):
            return real_replace(src, dst)  # copy_file's own rename stays local
        raise OSError(errno.EXDEV, "Invalid cross-device link")

    monkeypatch.setattr(os, "replace", replace)
    (tmp_path / "a").write_text("file")
    fs.move(tmp_path / "a", tmp_path / "b")
    assert (tmp_path / "b").read_text() == "file" and not (tmp_path / "a").exists()

    tree = tmp_path / "tree"
    (tree / "d").mkdir(parents=True)
    (tree / "d" / "f").write_text("deep")
    fs.move(tree, tmp_path / "moved")
    assert (tmp_path / "moved" / "d" / "f").read_text() == "deep" and not tree.exists()


def test_move_passes_other_errors_on(tmp_path, sandboxed):
    with pytest.raises(FileNotFoundError):
        fs.move(tmp_path / "missing", tmp_path / "x")


def _race(monkeypatch, tmp_path, fail_second=False):
    """Two writers to one target, held at their rename until both have a temporary file."""
    import threading

    real_replace = os.replace
    gate = threading.Barrier(2, timeout=10)
    seen = []
    lock = threading.Lock()

    def replace(src, dst):
        with lock:
            seen.append(str(src))
        gate.wait()
        if fail_second and threading.current_thread().name == "writer-b":
            raise OSError(errno.EIO, "refused")
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", replace)
    target = tmp_path / "dest.txt"
    outcomes = {}

    def write(name, text):
        try:
            outcomes[name] = fs.write_file(target, text)
        except BaseException as error:
            outcomes[name] = error

    threads = [threading.Thread(target=write, args=(n, n * 10), name=f"writer-{n}") for n in "ab"]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(15)
    return target, outcomes, seen


def test_concurrent_writers_of_one_target_never_share_a_temporary_file(tmp_path, monkeypatch):
    target, outcomes, seen = _race(monkeypatch, tmp_path)
    assert len(set(seen)) == 2
    assert all(not isinstance(value, BaseException) for value in outcomes.values()), outcomes
    assert target.read_text() in {"a" * 10, "b" * 10}
    assert [p.name for p in tmp_path.iterdir()] == ["dest.txt"]


def test_a_failed_writer_removes_only_its_own_temporary_file(tmp_path, monkeypatch):
    target, outcomes, seen = _race(monkeypatch, tmp_path, fail_second=True)
    assert isinstance(outcomes["b"], OSError) and outcomes["a"] == target
    assert target.read_text() == "a" * 10
    assert [p.name for p in tmp_path.iterdir()] == ["dest.txt"]


def test_a_written_file_is_flushed_before_the_rename(tmp_path, monkeypatch):
    events = []
    real_fsync, real_replace = os.fsync, os.replace
    monkeypatch.setattr(os, "fsync", lambda fd: (events.append("fsync"), real_fsync(fd))[1])
    monkeypatch.setattr(os, "replace", lambda a, b: (events.append("replace"), real_replace(a, b))[1])
    fs.write_file(tmp_path / "f", "x")
    fs.copy_file(tmp_path / "f", tmp_path / "g")
    assert events == ["fsync", "replace", "fsync", "replace"]
