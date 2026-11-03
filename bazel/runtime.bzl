"""Original Rust tests with Common's declared runtime and Cargo fixtures."""

load("@rules_shell//shell:sh_test.bzl", "sh_test")

def ha_rust_test(name, binary, fixtures = [], **kwargs):
    sh_test(
        name = name,
        srcs = ["@sonic_swss_common//tools/bazel/runtime:run_with_runtime.py"],
        args = [
            "$(location @patchelf//:patchelf)",
            "$(location //bazel:test_runtime)",
            "-",
            "enabled",
            "$(location //bazel:run_test_fixture.py)",
            "$(location @patchelf//:patchelf)",
            "$(location @sonic_swss_common//tools/bazel/runtime:run_with_runtime.py)",
            "$(location " + binary + ")",
            "$(location :Cargo.toml)",
            str(len(fixtures)),
        ] + ["$(location " + fixture + ")" for fixture in fixtures] + [
            "--test-threads=1",
        ],
        data = [
            binary,
            ":Cargo.toml",
            "//bazel:test_runtime",
            "//bazel:run_test_fixture.py",
            "@patchelf//:patchelf",
            "@sonic_swss_common//tools/bazel/runtime:run_with_runtime.py",
        ] + fixtures,
        tags = ["exclusive"],
        timeout = "long",
        **kwargs
    )
