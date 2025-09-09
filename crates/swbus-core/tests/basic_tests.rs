mod common;
use common::test_executor::{run_tests, TopoRuntime};
use sonic_common::log::init_logger_for_test;
#[tokio::test]
async fn test_b2b() {
    init_logger_for_test();

    let mut topo = TopoRuntime::new("tests/data/b2b/topo.json");
    topo.bring_up().await;
    // Can't split run_tests into multiple test cases. Each tokio::test creates a new runtime,
    // from which bring_up_topo runs. When a test case is done, the runtime is dropped and the
    // topology is torn down. It can't be reused to run another test case.
    // If move bring_up_topo outside of test cases to a setup function and create a single shared
    // runtime, test case cannot use the shared runtime. It will panic with "fatal runtime
    // error: thread::set_current should only be called once per thread".
    run_tests(&mut topo, "tests/data/b2b/test_ping.json", None).await;
    run_tests(&mut topo, "tests/data/b2b/test_show_route.json", None).await;
    run_tests(&mut topo, "tests/data/b2b/test_trace_route.json", None).await;
}
//Can't add another test that brings up a topology and runs tests unless the new topo has
// different ports than the existing ones. Otherwise, the tests will fail because the ports
// are already in use. This is because rust will run all tests in parallel.
