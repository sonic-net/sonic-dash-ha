#!/usr/bin/python3
"""Check the deployed HA payload, symbols and real private-loopback consumers."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import struct
import subprocess
import tarfile
import time
import zlib

EPOCH = 1686772800
PROGRAMS = {"hamgrd": "hamgrd", "swbusd": "swbusd", "swbus-cli": "swbus_cli"}
EXPECTED = {"usr/bin/" + name for name in PROGRAMS} | {"usr/bin/wait_for_loopback.py"}


def unpack(path, destination, expected, mode, epoch=EPOCH, directory_epochs=None):
    with tarfile.open(path) as archive:
        members = archive.getmembers()
        names = [m.name.removeprefix("./") for m in members]
        assert len(names) == len(set(names)), "Duplicate TAR member"
        files = {m.name.removeprefix("./") for m in members if m.isfile()}
        assert files == expected, (files, expected)
        for member in members:
            name = member.name.removeprefix("./").rstrip("/")
            assert member.uid == member.gid == 0, member.name
            expected_time = (directory_epochs or {}).get(name, epoch) if member.isdir() else epoch
            assert member.mtime == expected_time, member.name
            if member.isdir():
                assert member.mode == 0o755 and (name == "." or any(x.startswith(name + "/") for x in expected)), member.name
            else:
                assert member.isfile() and member.mode == mode, member.name
        archive.extractall(destination, filter="data")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-tar", type=Path, required=True)
    parser.add_argument("--debug-tar", type=Path, required=True)
    parser.add_argument("--script", type=Path, required=True)
    args = parser.parse_args()
    runtime = Path(os.environ["SWSS_RUNTIME_ROOT"])
    scratch = Path(os.environ["TEST_TMPDIR"]) / "installed-ha"
    scratch.mkdir()
    deployed = scratch / "runtime"
    symbols = scratch / "symbols"
    unpack(args.runtime_tar, deployed, EXPECTED, 0o755)
    assert (deployed / "usr/bin/wait_for_loopback.py").read_bytes() == args.script.read_bytes()
    # Common's declared runtime has the package and each transitive library.
    # Invoke its loader directly, with no host library search fallback needed
    # for any DT_NEEDED entry asserted below.
    loaders = {p.resolve() for p in runtime.rglob("ld-linux-x86-64.so.2") if p.is_file()}
    assert len(loaders) == 1
    loader = str(loaders.pop())
    libraries = sorted({str(p.parent.resolve()) for p in runtime.rglob("lib*.so*") if p.is_file()})
    library_path = ":".join(libraries)
    env = dict(os.environ, LC_ALL="C", DEBUGINFOD_URLS="", PYTHONHOME=str(runtime / "usr"))
    for variable in ("LD_PRELOAD", "LD_LIBRARY_PATH", "LD_AUDIT", "PYTHONPATH"):
        env.pop(variable, None)

    def executable(name):
        matches = {p.resolve() for p in (runtime / "usr/bin").glob("*" + name) if p.is_file()}
        exact = runtime / "usr/bin" / name
        if exact.is_file():
            return exact.resolve()
        assert len(matches) == 1, (name, matches)
        return matches.pop()

    def argv(program, *arguments):
        return [loader, "--library-path", library_path, str(program), *map(str, arguments)]

    def run(program, *arguments):
        result = subprocess.run(argv(program, *arguments), env=env, capture_output=True, text=True, timeout=40)
        assert result.returncode == 0, result.stdout + result.stderr
        return result.stdout + result.stderr

    readelf, objcopy, gdb = [executable(name) for name in ("readelf", "objcopy", "gdb")]
    ids = {}
    for name in PROGRAMS:
        binary = deployed / "usr/bin" / name
        header = binary.read_bytes()[:20]
        assert header[:6] == b"\x7fELF\x02\x01" and struct.unpack_from("<H", header, 18)[0] == 62
        notes = run(readelf, "-n", binary)
        ids[name] = re.search(r"Build ID: ([0-9a-f]+)", notes).group(1)
        assert ".debug_info" not in run(readelf, "-SW", binary)
        listing = subprocess.run(
            [loader, "--library-path", library_path, "--list", str(binary)], env=env,
            capture_output=True, text=True, check=True, timeout=40).stdout
        assert "not found" not in listing
        for dependency in re.findall(r"=> (\S+)", listing):
            assert Path(dependency).resolve().is_relative_to(runtime), dependency
        assert "Usage:" in run(binary, "--help")
    expected_debug = {"usr/lib/debug/.build-id/" + build_id[:2] + "/" + build_id[2:] + ".debug" for build_id in ids.values()}
    # tar.bzl's directory mutator gives these synthesized parents its fixed
    # default epoch; the owner symbol tree retains its own fixed epoch.
    unpack(args.debug_tar, symbols, expected_debug, 0o644, epoch=1672560000,
           directory_epochs={name: 946699200 for name in (".", "usr", "usr/lib", "usr/lib/debug")})
    report = {"programs": {}, "scope": "AMD64 runtime TAR, matching symbols and private loopback swbus; Docker/ASIC HA deployment is separate"}
    for name, crate in PROGRAMS.items():
        binary = deployed / "usr/bin" / name
        build_id = ids[name]
        debug = symbols / "usr/lib/debug/.build-id" / build_id[:2] / (build_id[2:] + ".debug")
        assert re.search(r"Build ID: ([0-9a-f]+)", run(readelf, "-n", debug)).group(1) == build_id
        assert ".debug_info" in run(readelf, "-SW", debug)
        section = scratch / (name + ".debuglink")
        run(objcopy, "--dump-section", ".gnu_debuglink=" + str(section), binary)
        data = section.read_bytes(); end = data.index(b"\0")
        assert data[:end].decode() == debug.name
        crc = struct.unpack_from("<I", data, (end + 4) & ~3)[0]
        assert crc == zlib.crc32(debug.read_bytes()) & 0xffffffff
        query = "info line " + crate + "::main"
        common = ["--batch", "--nx", "--quiet", "--data-directory=" + str(runtime / "usr/share/gdb"),
                  "-iex", "set auto-load off", "-iex", "set debuginfod enabled off"]
        empty = scratch / (name + "-no-symbols"); empty.mkdir()
        negative_result = subprocess.run(argv(gdb, *common, "-iex", "set debug-file-directory " + str(empty),
            "-ex", "file " + str(binary), "-ex", query), env=env, capture_output=True, text=True, timeout=40)
        negative = negative_result.stdout + negative_result.stderr
        assert negative_result.returncode == 1, negative
        assert 'Function "' + crate + '::main" not defined.' in negative, negative
        assert not re.search(r"Line \d+ of .*\.rs", negative), negative
        positive = run(gdb, *common, "-iex", "set debug-file-directory " + str(symbols / "usr/lib/debug"), "-ex", "file " + str(binary), "-ex", query)
        assert re.search(r"Line \d+ of .*crates/" + re.escape(name) + r"/src/main.rs", positive), positive
        report["programs"][name] = {"sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "build_id": build_id,
                                    "debug_sha256": hashlib.sha256(debug.read_bytes()).hexdigest(), "gdb": positive, "negative_gdb": negative}
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0)); port = reservation.getsockname()[1]
    config = scratch / "swbus.yaml"
    service = "region-a.cluster-a.127.0.0.1-dpu0"
    config.write_text(f'endpoint: "127.0.0.1:{port}"\nroutes:\n  - key: "{service}"\n    scope: "InCluster"\npeers: []\n')
    server_log = scratch / "swbusd.log"
    with server_log.open("w") as log:
        server = subprocess.Popen(argv(deployed / "usr/bin/swbusd", "--config", config), env=env, stdout=log, stderr=log)
        try:
            for attempt in range(100):
                assert server.poll() is None, server_log.read_text()
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.1):
                        break
                except OSError:
                    time.sleep(.1)
            else:
                raise AssertionError("Installed swbusd did not listen")
            output = run(deployed / "usr/bin/swbus-cli", "--config-file", config, "ping", "--count", "1", "--timeout", "3", "--interval", "0", service)
            assert "Response received: ping_seq=0" in output and "request timeout" not in output, output
            report["installed_swbus_ping"] = output
        finally:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill();server.wait(timeout=5)
    destination = Path(os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR", scratch)) / "installed-ha.json"
    destination.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
