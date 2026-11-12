#!/usr/bin/python3
"""Build and retain the native AMD64 HA source contract without producing DEBs."""
import functools
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tomllib
from urllib.parse import unquote, urlparse
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
PACKAGES = ["//:dash_ha_pkg", "//:dash_ha_pkg.debug_symbols", "//:runtime_packages.json"]
BUILD = PACKAGES + ["//crates/container:container"]
DOC_TEST = "//crates/swbus-actor:doc_test"
TESTS = ["//crates/...", "//bazel:installed_package_test", "@sonic_swss_common//tools/bazel/rust:testing_interface_test",
         "@sonic_swss_common//tools/bazel/rust:async_testing_interface_test", "@sonic_swss_common//crates/swss-common:swss_common_async_test"]


def profile_acceptance(profile, registry_head):
    output = ROOT / "artifacts" / profile
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    bazel = [env.get("BAZEL", "bazel"), "--host_jvm_args=-Xmx3g"]
    flags = ["--lockfile_mode=update", "--jobs=2"]
    downloader = None
    if env.get("BAZEL_DOWNLOADER_CONFIG"):
        downloader_path = Path(env["BAZEL_DOWNLOADER_CONFIG"]).resolve(strict=True)
        flags.append("--downloader_config=" + str(downloader_path))
        retained_config = output / "downloader.cfg"
        shutil.copyfile(downloader_path, retained_config)
        downloader = {"path": retained_config.name,
            "sha256": hashlib.sha256(retained_config.read_bytes()).hexdigest()}
    flags += ["--compilation_mode=dbg"] if profile == "dev" else ["--config=release"]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True)
    assert not status, "Build acceptance requires a clean source checkout: " + status
    assert not subprocess.check_output(["git", "ls-files", "MODULE.bazel.lock", "Cargo.Bazel.lock"], cwd=ROOT)
    input_hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in ("Cargo.toml", "Cargo.lock", "MODULE.bazel", ".bazelrc", "bazel/common.bazelrc")}

    def run(command, *arguments, capture=None, log_name=None, allow_failure=False):
        command_flags = [item for item in flags if item.startswith(("--lockfile_mode=", "--downloader_config="))] if command == "query" else flags
        argv = [*bazel, command, *command_flags, *arguments]
        if capture:
            with capture.open("w") as stream:
                subprocess.run(argv, cwd=ROOT, env=env, stdout=stream, check=True)
            return None
        if command in ("build", "test", "run"):
            log_path = output / (log_name or (command + ".log"))
            with log_path.open("w") as log:
                process = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                           stderr=subprocess.STDOUT, text=True)
                for line in process.stdout:
                    print(line, end="", flush=True)
                    log.write(line)
                status = process.wait()
                assert allow_failure or status == 0, "Bazel failed; see " + str(log_path)
            return status
        return subprocess.check_output(argv, cwd=ROOT, env=env, text=True)

    run("run", "//bazel/buildifier:check")
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]
    members = set(workspace["members"])
    definitions = run("query", 'kind("rust_(library|binary|proc_macro) rule", //crates/...)', "--output=label").splitlines()
    packages = {label.removeprefix("//").split(":")[0] for label in definitions}
    assert packages == members, ("Cargo/Bazel crate coverage differs", packages, members)
    native_install = set()
    for line in (ROOT / "debian/install").read_text().splitlines():
        source, directory = line.split()
        native_install.add(directory + "/" + Path(source).name)
    assert native_install == {"usr/bin/" + name for name in ("hamgrd", "swbusd", "swbus-cli", "wait_for_loopback.py")}
    (output / "source-contract.json").write_text(json.dumps({"cargo_crates": sorted(members),
        "bazel_targets": definitions, "native_install": sorted(native_install)}, indent=2) + "\n")

    def normalized(label):
        for prefix in ("@@sonic-swss-common+//", "@sonic-swss-common//"):
            if label.startswith(prefix):
                return "@sonic_swss_common//" + label[len(prefix):]
        return label
    selection = "tests(set(" + " ".join(TESTS) + "))"
    expected_tests = {normalized(label) for label in run("query",
        selection + ' except attr("tags", "manual", ' + selection + ')', "--output=label").splitlines()}
    common_tests = {normalized(label) for label in TESTS if label.startswith("@")}
    assert {label for label in expected_tests if label.startswith("@") } == common_tests
    assert DOC_TEST in expected_tests
    original_crate_tests = sorted(label for label in expected_tests if label.startswith("//crates/") and label != DOC_TEST)
    assert expected_tests - common_tests - {label for label in expected_tests if label.startswith("//crates/")} == {"//bazel:installed_package_test"}

    # Retain the complete configured closure. Omitting commandline serialization
    # bounds JVM memory; the graph still includes every transitive action/output.
    graph = output / "actions.json"
    run("aquery", "--include_commandline=false", "--output=jsonproto",
        "deps(" + " union ".join(BUILD + TESTS + ["//bazel:common_test_runner"]) + ")", capture=graph)
    data = json.loads(graph.read_text())
    fragments = {x["id"]: x for x in data["pathFragments"]}
    @functools.lru_cache(None)
    def path(identity):
        item = fragments[identity]
        return (path(item["parentId"]) + "/" if item.get("parentId") else "") + item["label"]
    artifacts = {x["id"]: path(x["pathFragmentId"]) for x in data["artifacts"]}
    emitted = [artifacts[i] for action in data["actions"] for i in action.get("outputIds", [])]
    assert not [name for name in emitted if name.endswith((".deb", ".udeb", ".ddeb"))]
    (output / "no-deb.json").write_text(json.dumps({"actions": len(data["actions"]), "outputs": len(emitted), "deb_outputs": [], "audit_scope": "Every configured action output; command lines are not serialized"}, indent=2) + "\n")
    run("build", "--build_event_json_file=" + str(output / "build-events.jsonl"), *BUILD)
    # Bazel 8 info does not initialize apparent platform repository mappings.
    # This path-only query does not analyze or build a configured target.
    execution = Path(run("info", "--platforms=", "execution_root").strip()).resolve()
    retained = {}
    for label in BUILD:
        names = run("cquery", label, "--output=files").splitlines()
        assert names, label
        for name in names:
            source = execution / name
            assert source.is_file(), (label, name)
            destination = output / "packages" / source.name
            destination.parent.mkdir(exist_ok=True)
            shutil.copyfile(source, destination)
            retained[str(destination.relative_to(output))] = {"target": label, "size": source.stat().st_size,
                "sha256": hashlib.sha256(source.read_bytes()).hexdigest()}
    # Retain successfully built payloads before tests, including on test failure.
    for name, digest in input_hashes.items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
        # upload-artifact excludes hidden files by default.
        destination = output / ("root.bazelrc" if name == ".bazelrc" else name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    shutil.copyfile(ROOT / "MODULE.bazel.lock", output / "MODULE.bazel.lock")
    (output / "build-provenance.json").write_text(json.dumps({"source": head,
        "architecture": "amd64", "configuration": profile, "registry_head": registry_head, "downloader_config": downloader,
        "source_inputs": input_hashes, "module_lock_sha256": hashlib.sha256((output / "MODULE.bazel.lock").read_bytes()).hexdigest(),
        "outputs": retained, "status": "built; test acceptance pending"}, indent=2) + "\n")
    groups = [
        ("ha", sorted(expected_tests - common_tests - {DOC_TEST}), []),
        ("docs", [DOC_TEST], []),
        ("common", sorted(common_tests), ["--run_under=//bazel:common_test_runner"]),
    ]
    statuses = []
    event_files = []
    for name, labels, extra in groups:
        events = output / (name + "-test-events.jsonl")
        event_files.append(events)
        statuses.append(run("test", "--nocache_test_results", "--test_output=errors",
            "--build_event_json_file=" + str(events), *extra, *labels,
            log_name=name + "-test.log", allow_failure=True))
    # Rustdoc runs in a build action; its native result files are independent
    # of the Bazel test runner's replayed XML/log. Retain both before switching
    # compilation modes, including the original failure output.
    doc_outputs = {}
    doc_binary = run("cquery", DOC_TEST, "--output=files").splitlines()
    assert len(doc_binary) == 1, doc_binary
    for suffix in ("stdout", "stderr", "exit_code"):
        source = execution / (doc_binary[0] + "." + suffix)
        assert source.is_file(), source
        destination = output / "rustdoc" / source.name
        destination.parent.mkdir(exist_ok=True)
        shutil.copyfile(source, destination)
        doc_outputs[suffix] = {"path": str(destination.relative_to(output)),
            "sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "size": source.stat().st_size}
    (output / "rustdoc-results.json").write_text(json.dumps(doc_outputs, indent=2) + "\n")
    tests = {}
    attempts = []
    identities = set()
    failures = []
    for line in (line for event_file in event_files for line in event_file.open()):
        event = json.loads(line)
        if "testResult" not in event:
            continue
        identity, result = event["id"]["testResult"], event["testResult"]
        if result["status"] != "PASSED":
            failures.append({"identity": identity, "status": result["status"]})
        label = normalized(identity["label"])
        assert label in expected_tests, identity
        key = json.dumps(identity, sort_keys=True)
        assert key not in identities, identity
        identities.add(key)
        # Retain configuration/run/shard/attempt independently, as the shared
        # registry collector does. Never flatten transitioned tests by label.
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        attempt = {"identity": identity, "label": label, "status": result["status"], "files": []}
        xmls = []
        for index, artifact in enumerate(result.get("testActionOutput", [])):
            uri = urlparse(artifact.get("uri", ""))
            assert uri.scheme == "file" and uri.netloc in ("", "localhost") and not uri.query and not uri.fragment
            source = Path(unquote(uri.path)).resolve()
            assert source.is_relative_to(execution) and source.is_file(), source
            destination = output / "test-results" / key_hash / (str(index) + "-" + source.name)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
            attempt["files"].append({"name": artifact["name"], "path": str(destination.relative_to(output)),
                "source": str(source.relative_to(execution)), "size": source.stat().st_size,
                "sha256": hashlib.sha256(source.read_bytes()).hexdigest()})
            if artifact["name"] == "test.xml":
                tree = ET.parse(source)
                if any(list(tree.iter(kind)) for kind in ("failure", "error", "skipped")) or not list(tree.iter("testcase")):
                    failures.append({"identity": identity, "xml": "Failed, skipped or empty test XML"})
                xmls.append(str(destination.relative_to(output)))
        assert len(xmls) == 1, label
        tests.setdefault(label, []).append(xmls[0])
        attempts.append(attempt)
    assert set(tests) == expected_tests, ("Missing or unexpected tests", sorted(expected_tests - set(tests)), sorted(set(tests) - expected_tests))
    (output / "test-results.json").write_text(json.dumps({"expected_labels": sorted(expected_tests), "original_crate_test_count": len(original_crate_tests), "attempts": attempts, "failures": failures}, indent=2) + "\n")
    assert not failures and all(status == 0 for status in statuses), (failures, statuses)
    for name, digest in input_hashes.items():
        assert hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest, name
    assert not subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)
    (output / "provenance.json").write_text(json.dumps({"source": head, "architecture": "amd64", "configuration": profile,
        "registry_head": registry_head, "downloader_config": downloader, "source_inputs": input_hashes, "module_lock_sha256": hashlib.sha256((output / "MODULE.bazel.lock").read_bytes()).hexdigest(),
        "outputs": retained, "tests": tests, "rustdoc": doc_outputs,
        "scope": "HA source runtime/debug TARs and original tests; Docker integration is separate"}, indent=2) + "\n")


def main():
    # Record the observed canonical registry revision without changing the root
    # .bazelrc's main+BCR selection or bypassing workspace/local configuration.
    registry_head = subprocess.check_output([
        "git", "ls-remote", "https://github.com/securely1g/sonic-bazel-registry.git", "refs/heads/main",
    ], cwd=ROOT, text=True).split()[0]
    for profile in ("dev", "release"):
        profile_acceptance(profile, registry_head)


if __name__ == "__main__":
    main()
