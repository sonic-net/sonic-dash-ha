"""Use SWSS Common's Rust build definitions with the sources in DASH HA's Cargo.lock."""

def _swss_rust_impl(ctx):
    common = "crates/swss-common"
    testing = "crates/swss-common-testing"
    for directory, git_build in [(common, ctx.attr.common_source), (testing, ctx.attr.testing_source)]:
        ctx.symlink(ctx.path(git_build).dirname.get_child("src"), directory + "/src")

    # Bindgen and the C++ shared library come from the same native build.
    # Both Rust variants and the testing helper use the DASH HA Cargo graph.
    ctx.template(common + "/BUILD.bazel", ctx.attr.common_build, substitutions = {
        "//:libswsscommon_shared": "@sonic_swss_common//:libswsscommon_shared",
        "//tools/bazel/rust:bindings.bzl": "@sonic_swss_common//tools/bazel/rust:bindings.bzl",
        ":bindings_dir": "@sonic_swss_common//crates/swss-common:bindings_dir",
        "@sonic_rust_deps//:libc": "@sonic_rust_deps//:swss-libc",
    }, executable = False)
    ctx.symlink(ctx.path(ctx.attr.common_build).dirname.get_child("locked_source.bzl"), common + "/locked_source.bzl")
    ctx.symlink(ctx.path(ctx.attr.testing_build), testing + "/BUILD.bazel")
    for path in ctx.path(ctx.attr.owner_tests).dirname.readdir():
        if path.basename != "BUILD.bazel":
            ctx.symlink(path, "tools/bazel/rust/" + path.basename)
    ctx.template("tools/bazel/rust/BUILD.bazel", ctx.attr.owner_tests, substitutions = {
        "@sonic_rust_deps//:serde_core": "@sonic_rust_deps//:swss-serde-core",
    }, executable = False)

swss_rust = repository_rule(
    implementation = _swss_rust_impl,
    attrs = {
        "common_build": attr.label(default = "@sonic_swss_common//crates/swss-common:BUILD.bazel"),
        "common_source": attr.label(default = "@dash_ha_swss_sources//crates/swss-common:BUILD.bazel"),
        "owner_tests": attr.label(default = "@sonic_swss_common//tools/bazel/rust:BUILD.bazel"),
        "testing_build": attr.label(default = "@sonic_swss_common//crates/swss-common-testing:BUILD.bazel"),
        "testing_source": attr.label(default = "@dash_ha_swss_sources//crates/swss-common-testing:BUILD.bazel"),
    },
)
