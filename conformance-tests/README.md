Most of the test here are from https://github.com/temporalio/temporal/tree/main/tests.
The test was updated to:
- Injected the Tempura Proxy Lifecycle (testcore/test_cluster.go)
Instead of just spinning up an in-memory Temporal cluster, we augmented the cluster initialization to also deploy the Tempura proxy alongside it:

- Add proxy compilation: and miniredis integration: Embedded miniredis to act as the namespace mapping database for the proxy during the test's lifecycle.

- Namespace Seeding: Automatically populates the mocked Redis instance with the routing rule (tempura:namespace_routing:<namespace> -> 127.0.0.1:<temporal-port>) for each test cluster.
- Rerouted SDK Client Traffic to the Proxy (testcore/functional_test_base.go)
- Modified the setupCluster method to intercept the SDK client configuration before the tests begin.
- Changed clientOptions.HostPort to point to the Tempura proxy's port instead of the actual Temporal frontend port. This forces all test SDK calls to route through our proxy layer first.
- Replaced Remote Imports: The original test files (*_test.go) imported "go.temporal.io/server/tests/testcore". This meant they were bypassing our customized testcore and using the standard remote one. We ran a mass find-and-replace across all 91 test files to import "github.com/phuongdnguyen/tempura/conformance-tests/testcore" and xdc instead.
- Reconciled Breaking API Changes with Upstream (testcore):
    - Context Handling: Updated testcore/context.go to use testcontext.For and testcontext.AttachDecorator (replacing the deprecated testcontext.New and testcontext.WithContextDecorator).
    - GRPC Interceptors: Stripped out grpcClientInterceptor assignments in onebox.go, functional_test_base.go, and test_env.go since the grpcinject component was removed in upstream Temporal tests.
    - Disabled Incompatible Tests: Renamed three bleeding-edge tests (timeskipping_bound_test.go, timeskipping_propagation_test.go, and timeskipping_test.go) to .disabled because they referenced TimeSkippingConfig fields that don't exist in the currently pinned version of go.temporal.io/api.