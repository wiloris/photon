<!--
Photon — cross-platform core platform.
Keep this page focused on the public project: architecture, entry points, build, and honest limitations.
-->

<p align="center">
  <img src="assets/branding/photon-logo.png" alt="Photon logo" width="360">
</p>

<h1 align="center">Photon</h1>

<p align="center">
  <strong>A platform core for client applications.</strong><br>
  Transport, identity, cryptography, storage, plugins, and developer tooling.
</p>

<p align="center">
  <a href="README.md">Русский</a> ·
  <a href="ARCHITECTURE.md">Architecture</a> ·
  <a href="ROADMAP.md">Roadmap</a> ·
  <a href="CONTRIBUTING.md">Contributing</a>
</p>

---

Photon is a standalone Go platform for building client applications. The core is separated from the user interface and any single application. It exposes a compact C ABI and a JVM SDK, with diagnostic tooling and a separate plugin runtime around it.

**ENGRAM is an application built on Photon, not part of the core.** Capabilities useful to multiple clients belong in Photon; ENGRAM-specific behavior belongs in the application or its extensions. This separation is a core architectural principle.

## Capabilities

- **Go core.** Services, ports, and adapters help keep application logic separate from concrete integrations.
- **Network transport.** QUIC and TCP components are present; the current working path and limitations are described below and in the documentation.
- **Identity and cryptography.** Identity components, cryptographic primitives, and OIDC integration.
- **Storage and data exchange.** Local storage, events, an outbox, media, and peer-to-peer components.
- **C ABI and JVM SDK.** A small external boundary makes the core accessible from other languages; the JVM layer supports Java and Kotlin.
- **Plugins.** Separate processes, IPC, manifests, and permissions keep plugin code distinct from the core.
- **Diagnostics.** Core journal, Inspector Live, and tools for exploring networking, storage, cryptography, and plugins.

## Repository map

| Component | Directory | Purpose |
|---|---|---|
| Photon Core | `photon-core/` | Go core: services, ports and adapters, transport, storage, diagnostics |
| C ABI and JNI | `photon-bindings/` | C interface and JVM bridge |
| JVM SDK | `photon-sdk/` | Client API for Java/Kotlin |
| Plugin runtime | `photon-plugin-runtime/`, `photon-plugin-sdk/` | Plugin execution and development SDK |
| Developer tools | `photon-devtools-core/`, `photon-developer-tools/` | Diagnostics and Linux Desktop developer client |
| QUIC Bridge | `photon-quic-bridge/` | Mattermost integration plugin and related specifications |

## Getting started

### Requirements

- Go version varies by module; use Go 1.22 or later for the main module set.
- JDK 21 and Gradle for JVM modules.
- A C compiler for modules using cgo.
- `protoc` to build `photon-developer-tools`.
- `photon-core/tests/transportlab/bridgesim` requires Go 1.24 or later. With `GOTOOLCHAIN=auto`, Go can download the required toolchain automatically.

### Build and test

Run these commands from the repository root:

```sh
(cd photon-core && go build ./... && go test ./...)
(cd photon-bindings/c && go test .)
(cd photon-plugin-sdk && go test ./...)
(cd photon-plugin-runtime && go test -timeout 600s ./...)
(cd photon-sdk && gradle test)
(cd photon-devtools-core && gradle test)
(cd photon-developer-tools && gradle test)
```

The last command builds the native core library and JNI library. Dependencies and test availability vary by module; check each module's documentation for its current requirements.

## Usage

- **C and native integrations:** `photon-bindings/c/photon_core.h` and the [C ABI README](photon-bindings/c/README.md).
- **Java and Kotlin:** [SDK API documentation](photon-bindings/docs/sdk-api.md).
- **Plugin development:** [plugin development guide](photon-plugin-runtime/docs/plugin-development.md) and examples in `photon-plugin-runtime/examples/`.
- **Architecture:** [ARCHITECTURE.md](ARCHITECTURE.md).
- **Module versioning:** [VERSIONING.md](VERSIONING.md).

Go modules are versioned independently. Tags include the module directory, for example `photon-core/vX.Y.Z`. Local `replace` directives support coordinated development inside this repository; they do not apply to consumers of released modules.

## Status and limitations

Photon is an actively developed platform, and components have different maturity levels. The roadmap items below should not be read as shipped features.

- **A standalone server is not included in this repository.** The protocol evolved alongside the first server integration, a Mattermost plugin; `bridgesim` is used for transport tests.
- **The working network path currently uses QUIC.** The full Autodial sequence (IPv6/IPv4, QUIC/TCP, and an obfuscated transport) is not yet wired into one working path; the TCP server side and obfuscated transport are unfinished.
- **Plugin process isolation is supported on Linux.**
- **Linux x86-64 is the primary tested platform.** Android, macOS, and Windows have different maturity levels; consult the [roadmap](ROADMAP.md).

For implementation details and current decisions, see `photon-core/docs/`, `photon-bindings/docs/`, `photon-plugin-runtime/docs/`, and the [decision log](DECISIONS.md). Internal working documents such as `CURRENT_TASK.md` are primarily for development and may change with the active task.

## Brand and logo

Naming, logo usage, and visual guidance are documented in [BRAND.md](BRAND.md). The logo asset is stored at `assets/branding/photon-logo.png`.

## Contributing and security

Before opening a pull request, read [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md). Report potential vulnerabilities privately as described in [SECURITY.md](SECURITY.md), not in public issues.

## License

Photon is distributed under **GNU AGPL v3 only (`AGPL-3.0-only`)**. See [LICENSE](LICENSE) for the full text, [NOTICE](NOTICE) for additional notices, and [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md) for third-party components. This summary does not replace the license text; review its terms, including source-availability requirements for distribution and network use of modified versions.
