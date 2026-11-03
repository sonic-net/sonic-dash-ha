"""Use Common's declared runtime for its original top-level compatibility tests."""
import os
from pathlib import Path
import sys

from python.runfiles import runfiles


def main():
    files = runfiles.Create()
    assert files is not None

    def locate(name):
        path = files.Rlocation(name)
        assert path and Path(path).is_file(), name
        return str(Path(path).absolute())

    # Repository mapping keeps these labels valid both in the owning workspace
    # and in an ordinary external HA module. The test binary itself is supplied
    # by Bazel's --run_under contract, not a dependency on a private owner target.
    runner = locate("sonic_swss_common/tools/bazel/runtime/run_with_runtime.py")
    patchelf = locate("patchelf/patchelf")
    runtime = locate("sonic-dash-ha/bazel/test_runtime.tar")
    os.execv(sys.executable, [sys.executable, runner, patchelf, runtime, "-", "enabled", *sys.argv[1:]])


if __name__ == "__main__":
    main()
