#!/usr/bin/python3
"""Run original crate tests with declared tools and their Cargo fixture directory."""
import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys


def main():
    patchelf, common_runner, binary, manifest, count, *remaining = sys.argv[1:]
    fixtures, arguments = remaining[:int(count)], remaining[int(count):]
    patchelf, common_runner, binary = [str(Path(value).resolve()) for value in (patchelf, common_runner, binary)]
    # Keep the manifest's lexical runfiles location. Resolving its symlink
    # would escape into the source checkout and admit undeclared siblings.
    crate = Path(os.path.abspath(manifest)).parent
    spec = importlib.util.spec_from_file_location("common_runtime", common_runner)
    runtime = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runtime)
    root = Path(os.environ["SWSS_RUNTIME_ROOT"])
    # ProducerStateTable loads Common's real owner-provided Redis scripts.
    # Keep the installed default untouched and select this private test root.
    lua = root / "usr/share/swss"
    if not (lua / "producer_state_table_apply_view.lua").is_file():
        raise AssertionError("Declared Common Lua runtime is missing")
    os.environ["SWSS_COMMON_LUA_DIR"] = str(lua)
    temporary = Path(os.environ["TEST_TMPDIR"])
    tools = temporary / "ha-test-tools"
    tools.mkdir()
    # Common's original helper invokes these names. Retain its timeout, Redis
    # options and cleanup semantics, but execute only the declared package bytes.
    for name in ("redis-server", "timeout", "kill"):
        source = root / "usr/bin" / name
        if not source.is_file():
            source = root / "bin" / name
        if not source.is_file():
            raise AssertionError("Missing declared test tool: " + name)
        interpreter = runtime.elf_interpreter(source)
        loaders = {item.resolve() for item in root.rglob(Path(interpreter).name) if item.is_file()}
        if len(loaders) != 1:
            raise AssertionError("Ambiguous declared loader for " + name)
        installed = tools / name
        shutil.copyfile(source, installed)
        installed.chmod(0o755)
        subprocess.run([patchelf, "--set-interpreter", str(loaders.pop()), "--set-rpath", os.environ["LD_LIBRARY_PATH"], str(installed)], check=True)
    os.environ["PATH"] = str(tools)
    working = temporary / "ha-crate-fixtures"
    working.mkdir()
    for fixture in fixtures:
        source = Path(os.path.abspath(fixture))
        relative = source.relative_to(crate)
        if relative.parts[0] != "tests" or not source.is_file():
            raise AssertionError("Invalid declared crate fixture: " + fixture)
        destination = working / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    command = runtime.declared_runtime_command([binary, *arguments], root, patchelf)
    os.chdir(working)
    return subprocess.run(command).returncode


if __name__ == "__main__":
    raise SystemExit(main())
