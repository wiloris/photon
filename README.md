<div align="center">
  <img src="https://raw.githubusercontent.com/wiloris/photon/assets/photon-geometric-emblem/assets/brand/photon-geometric-emblem.png" alt="Photon geometric emblem" width="180" />
  <h1>PHOTON</h1>
  <p><strong>A modular core for connected applications.</strong></p>
  <p>Go core · C ABI · JVM SDK · Developer tools · Plugin runtime</p>
  <p>
    <a href="README.md">English</a> · <a href="README.ru.md">Русский</a>
  </p>
</div>

---

Photon is a platform for client applications: a Go core with storage,
identity, cryptography, QUIC transport, peer-to-peer connections and built-in
diagnostics; a single small C ABI as its outer boundary; and, on top of it, a
JVM SDK, developer tools and a plugin runtime.

> **Status: early public release.** The core interfaces are stable (C ABI 1.0),
> but the core network protocol is still tied to its first server
> implementation, and this repository contains no standalone server — see
> [Limitations](#limitations).

## What is here

| component | directory | what it does |
|---|---|---|
| Photon Core | `photon-core/` | ports and adapters: QUIC and TCP, bbolt storage, AES-GCM, Ed25519, OIDC, P2P (ICE + DTLS + SCTP on pion); services for sessions, action dispatch, the diagnostic journal, a local feed, an outbox and media |
| C ABI | `photon-bindings/c/` | 12 `Photon_*` functions, ABI 1.0 (`photon_core.h`) |
| JNI bridge | `photon-bindings/jvm/` | Java/Kotlin on top of the C ABI |
| JVM SDK | `photon-sdk/` | `dev.photon.sdk`: `PhotonClient`, configuration, events, local feed, `diagnostics()` |
| Plugins | `photon-plugin-runtime/`, `photon-plugin-sdk/` | plugins run as separate processes with IPC, a manifest and permissions; the plugin SDK has no external dependencies |
| Developer tools | `photon-devtools-core/`, `photon-developer-tools/` | Inspector Live, connection path, core journal; console labs for network, storage, cryptography and plugins. `photon-developer-tools` is the first Photon test client (Linux, JVM) |

Details: [`ARCHITECTURE.md`](ARCHITECTURE.md) (in Russian). Specifications:
`photon-core/docs/`, `photon-bindings/docs/`, `photon-plugin-runtime/docs/`.
Most design documents and code comments are currently in Russian.

## Build and test

Requirements: Go 1.22 (core, bindings, plugins), JDK 21 and Gradle (JVM
modules), a C compiler for cgo (`photon-bindings/c`), and `protoc` for
`photon-developer-tools`. The test server
`photon-core/tests/transportlab/bridgesim` needs Go ≥ 1.24, which
`GOTOOLCHAIN=auto` downloads automatically.

```sh
(cd photon-core && go build ./... && go test ./...)
(cd photon-bindings/c && go test .)
(cd photon-plugin-sdk && go test ./...)
(cd photon-plugin-runtime && go test -timeout 600s ./...)
(cd photon-sdk && gradle test)
(cd photon-devtools-core && gradle test)
(cd photon-developer-tools && gradle test)   # builds the core .so and JNI library
```

## Usage

Work with the core through the C ABI (`photon-bindings/c/photon_core.h`,
`photon-bindings/c/README.md`) or from the JVM through the SDK
(`photon-bindings/docs/sdk-api.md`). For plugins, see
`photon-plugin-runtime/docs/plugin-development.md` and the examples in
`photon-plugin-runtime/examples/`.

### Go modules

| module | version tag | depends on |
|---|---|---|
| `github.com/wiloris/photon/photon-core` | `photon-core/vX.Y.Z` | — |
| `github.com/wiloris/photon/photon-plugin-sdk` | `photon-plugin-sdk/vX.Y.Z` | — (no dependencies at all) |
| `github.com/wiloris/photon/photon-plugin-runtime` | `photon-plugin-runtime/vX.Y.Z` | `photon-core`, `photon-plugin-sdk` |
| `github.com/wiloris/photon/photon-bindings/c` | `photon-bindings/c/vX.Y.Z` | `photon-core` |

Each module is versioned separately by a tag prefixed with its directory, so
`go get github.com/wiloris/photon/photon-core@v0.1.0` resolves the tag
`photon-core/v0.1.0`. The `require` lines between the modules name released
versions. Inside the repository the modules also carry relative `replace`
directives, so that a change to `photon-core` is built and tested together with
the modules that use it; `replace` applies only to builds inside this
repository and is ignored when a module is used as a dependency. One
consequence: `go install <command>@<version>` refuses commands from
`photon-plugin-runtime` (for example `cmd/engram-plugin-host`), because their
module has `replace` directives; add the module to your own `go.mod` and use
`go build`, or build from a clone.

`photon-core/tests/transportlab/bridgesim` is a separate module only because it
needs a newer Go and quic-go; it is a test server and is not versioned.

How versions are chosen, what a release must contain and what may never be
done with a published version: `VERSIONING.md`.

## Limitations

- **No standalone server.** The core network protocol grew around its first
  server implementation — a Mattermost plugin that is not part of this
  repository; `photon.proto` contains Mattermost entities and actions of the
  first application built on the platform. For tests there is only the test
  server `bridgesim`.
- **The working transport path is QUIC only.** Autodial (the IPv6/IPv4 ×
  QUIC/TCP ladder ending in an obfuscated transport) is implemented but not
  wired in; there is no TCP server side; there is no obfuscated transport.
- **Plugin process isolation exists on Linux only.**
- **Tested platform: Linux x86-64.** Android, macOS and Windows are plans (see
  below).

## Roadmap (not implemented)

- **Android** — packaging the core (AAR) and an Android extension runtime.
- **Linux, macOS** — built and tested C ABI and SDK for desktop systems (Linux
  is tested today; on macOS the core only cross-compiles).
- **Photon Server** — a dedicated server for the protocol; a prerequisite is a
  server-neutral protocol.
- **Transport** — Autodial in the working path, a TCP server, an obfuscated
  transport.

## Contributing

Contributions require accepting the [Contributor License Agreement](CLA.md)
before a pull request is merged; the process is described in
[`CONTRIBUTING.md`](CONTRIBUTING.md). Please report security issues privately
as described in [`SECURITY.md`](SECURITY.md), not in public issues.

## License

GNU Affero General Public License version 3 only (`AGPL-3.0-only`); later
versions of the license are not offered. Copyright holder: wiloris. Full text:
[`LICENSE`](LICENSE); notice: [`NOTICE`](NOTICE).

In short (the license text prevails over this summary):

- if you distribute Photon or a work based on it — including an application
  that bundles the built core (`libphoton_core.so`), the SDK or the plugin SDK —
  you must give recipients the complete corresponding source code under
  `AGPL-3.0-only`;
- if a modified version lets users interact with it over a network, you must
  offer those users its source code (section 13);
- derivative works are distributed under `AGPL-3.0-only` only; there are no
  exceptions for particular products.

Third-party components and their licenses:
[`THIRD_PARTY_LICENSES.md`](THIRD_PARTY_LICENSES.md).

References like `ADR-NNN` in the code and documents point to the project's
internal decision log, which is not part of this release.
