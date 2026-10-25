# Versioning and releases

This document is the release contract of the repository. It says what has a
version, where that version lives, how it changes, and what may never be done
with a published release. The rules are checked by
`tools/versioning/check_versions.py`, which runs in GitHub Actions
(`.github/workflows/versioning.yml`) and is the mandatory gate before a release
tag is pushed.

## 1. What has a version

A component version, an interface version and a protocol version are
different things with different owners and different rates of change. None of
them is derived from another one, and equal or similar numbers never mean
compatibility.

### Released components

| component | version source | current |
|---|---|---|
| `github.com/wiloris/photon/photon-core` | Git tag `photon-core/vX.Y.Z` | v0.1.0 |
| `github.com/wiloris/photon/photon-plugin-sdk` | Git tag `photon-plugin-sdk/vX.Y.Z` | v0.1.0 |
| `github.com/wiloris/photon/photon-plugin-runtime` | Git tag `photon-plugin-runtime/vX.Y.Z` | v0.1.0 |
| `github.com/wiloris/photon/photon-bindings/c` | Git tag `photon-bindings/c/vX.Y.Z` | v0.1.0 |

The **Git tag is the only source of a module's version.** This is how Go
modules are versioned; the sources contain no version number of the module,
and none may be added: a second copy is a copy that can disagree. The four
tags of the first release point at commit `e4a80e0`; the module hashes
computed from these tags equal the records of `sum.golang.org`.

Each module is released on its own: a release of one module does not require
a release of the others, and module versions are not kept in step.

### Not released, therefore without a version

- `photon-core/tests/transportlab/bridgesim` — a test server; a separate Go
  module only because it needs a newer Go and quic-go. It is never tagged.
- `photon-sdk`, `photon-devtools-core`, `photon-developer-tools` — Gradle
  projects. Maven coordinates and publication are deferred, so they declare no
  `version`. Publishing one of them is a new component and needs its version
  source defined here first.
- Native libraries (`.so`) and binaries are built from the sources and are not
  published as release artifacts.
- **Photon as a product** has no version of its own today. If one is needed
  to name a set of components known to work together, it becomes a separate
  document listing the exact component and protocol versions of the set; the
  product version is never inferred from component numbers, and component
  numbers are never aligned to it.
- **Photon Server** is not part of this repository and has not been released.
  When it is, it gets its own version source and history under these rules.

### Interface and protocol versions

These live in the sources, because peers exchange or compare them at run time.
They are integers or MAJOR.MINOR pairs, not SemVer, and they change only when
the contract they name changes.

| boundary | declared in | checked by |
|---|---|---|
| C ABI (Core ↔ binding) | `PHOTON_ABI_MAJOR` / `PHOTON_ABI_MINOR` in `photon-bindings/c/photon_core.h` (mirrored in `internal_cgo_types.h` for cgo); `Photon_GetABIVersion()` | the client at initialisation: a different MAJOR is refused (`photon-bindings/jvm/kotlin/Photon.kt`, `EXPECTED_ABI_MAJOR`) |
| client ↔ server session protocol | `ProtocolVersion` in `photon-core/services/session/codec.go`, sent as `protocol_version` in the handshake; ALPN `photon-quic-v1` (`photon-core/adapters/quic/quic.go`) | the client after the handshake: a different declared version fails with `ErrProtocolVersion` (`photon-core/docs/bridge-session.md` §5) |
| plugin ↔ runtime IPC | `SupportedProtocolVersion` in `photon-plugin-runtime/manifest/types.go` (manifest `runtime.protocol_version`, `hello`) | the manifest validator and the IPC handshake: an exact match is required |
| plugin manifest format | `SupportedManifestVersion` in the same file | the manifest validator |

## 2. Semantic Versioning of components

Component versions are `vMAJOR.MINOR.PATCH` ([SemVer 2.0.0](https://semver.org/),
with the `v` prefix Go requires). The public contract of a Go module is the
exported API of its non-`internal` packages and the behaviour documented for
it; for `photon-bindings/c` it also includes the C header `photon_core.h`.

From `v1.0.0` on:

- **PATCH** — backward-compatible bug fixes; no new API.
- **MINOR** — new backward-compatible functionality.
- **MAJOR** — an incompatible change of the public contract. For Go modules a
  new MAJOR from 2 on also changes the module path (`.../photon-core/v2`); the
  check enforces it.

Before `v1.0.0` (all modules today) the public API may still change, and the
SemVer rule for `0.y.z` is applied this way:

- **0.y.(z+1)** — backward-compatible fixes only;
- **0.(y+1).0** — anything else: new functionality *or* an incompatible change.
  An incompatible change is named as such in the release notes.

`v1.0.0` is a deliberate decision that the public contract is stable, not the
next number after `v0.9.x`.

### Pre-releases

A pre-release is `vX.Y.Z-alpha.N`, `vX.Y.Z-beta.N` or `vX.Y.Z-rc.N`:

- `N` starts at 1 and grows by one without gaps;
- stages go forward only, alpha → beta → rc; a stage may be skipped;
- pre-releases of `X.Y.Z` are published before `X.Y.Z` itself, never after;
- no other suffixes and no build metadata (`+…`).

### Sequence

A version is the next one in its module's history: the next PATCH of an
existing MINOR line, the next MINOR of an existing MAJOR, or the next MAJOR —
no gaps. The first version of a module is `v0.1.0` or `v1.0.0` (or a
pre-release of them). A fix of an older line is allowed (`v0.1.1` after
`v0.2.0`), a repeated or lower version is not.

## 3. Protocol versions

Protocol and interface versions are governed separately from component
versions:

- A component release does not by itself change a protocol version, and a
  protocol change is not hidden inside a component number.
- **Compatible extension** — a new optional field, a new action code that an
  older peer answers with its usual "unsupported", a new event type that an
  older peer ignores — keeps the protocol version.
- **Incompatible change** — a field changes meaning or becomes mandatory, a
  framing or encoding change, removal of something a peer relies on —
  requires a new protocol version (for the C ABI: a new `PHOTON_ABI_MAJOR`),
  or an explicitly specified migration mechanism. The change is specified
  before it is implemented.
- **Compatibility is checked explicitly.** Each peer declares the version it
  speaks, and the other side refuses a version it does not support with a
  distinct error *before* any operation that depends on it. Component version
  numbers are never used to decide compatibility.

Known gap, recorded rather than hidden: the session handshake accepts a server
that does not send `protocol_version` at all and treats it as version 1,
because the deployed server predates the field (`bridge-session.md` §5). With
such a server compatibility is assumed, not proven.

**The wire format today is JSON over QUIC** (`photon-quic-v1`);
`photon-core/protobuf/photon.proto` describes the schema, it is not the wire
encoding. A binary Protocol Buffers wire protocol (over QUIC, with TCP + TLS 1.3
as the fallback) is separate future work. When it comes, it is a new protocol
version with its own ALPN and is negotiated explicitly; it is not introduced by
a component release.

## 4. Published releases are immutable

A published version names one content forever.

- A release tag is never moved, deleted or recreated. The repository rulesets
  forbid updating and deleting `*/v*` tags; the Go checksum database
  (`sum.golang.org`) keeps the hash of every fetched version permanently, so a
  version cannot be reused even after a tag disappears.
- A wrong release is fixed by the next version. A version that must not be
  used is withdrawn with a `retract` directive in the module's `go.mod` of a
  later release, and stays published.
- If binary artifacts are ever published, they are attached to the GitHub
  Release of their tag together with a `SHA256SUMS` file, and an attached
  artifact is never replaced.

## 5. Release notes

Each released module has `CHANGELOG.md` next to its `go.mod`. Changes go under
`## Unreleased`; at release that heading becomes `## vX.Y.Z — YYYY-MM-DD` in
the commit the tag points at. Every tagged version has a dated, non-empty
section, and no section names a version that has not been tagged.

A GitHub Release is required when there are artifacts to attach; for a
source-only Go module release it is optional and, if created, carries the same
text as the changelog section. The first release (`v0.1.0`) has no GitHub
Releases; its changelog sections were written after it, when release notes
became mandatory.

## 6. Who releases and how

A version is chosen at release time by whoever cuts the release, from the most
significant change in the module since its previous tag — not bumped on every
commit. The release is made from `main`:

1. Move the module's `## Unreleased` notes under `## vX.Y.Z — YYYY-MM-DD`.
2. If a dependent module should use the new version, its `require` line moves
   only to a tagged version: release dependencies first (`photon-core` and
   `photon-plugin-sdk` before `photon-plugin-runtime` and `photon-bindings/c`).
3. Commit to `main` and push the commit.
4. Run the release gate; it must pass:
   `python3 tools/versioning/check_versions.py release <dir>/vX.Y.Z --remote origin --sumdb`
5. Create the annotated tag with the exact message the check expects:
   `git tag -a <dir>/vX.Y.Z -m "<dir> vX.Y.Z"` and push it.
6. The workflow re-checks the pushed tag (`release --pushed`).

Step 4 is mandatory because CI runs only after a tag is pushed, and a pushed
tag cannot be changed any more.

## 7. Forbidden

- Publishing a different content under a used version, by moving, deleting or
  recreating a tag, or by replacing an artifact.
- Tagging a version that skips the sequence, goes below a released version,
  or is a pre-release of a version already released.
- Lightweight tags, tags without a module directory prefix, tags of modules
  that are not released, tags of commits outside `main`.
- A `require` on another module of this repository at a version that is not
  tagged (a pseudo-version or a planned version).
- Writing a module's version into its sources or build files.
- Releasing without a release-notes section for the version.
- Concluding that components are compatible from their version numbers.

## 8. What the check verifies

`check_versions.py check` reads the committed state (`HEAD`) and all tags:

- every tag is `<dir>/vX.Y.Z[-alpha|beta|rc.N]`, annotated, with the message
  `<dir> vX.Y.Z`, on a commit of `main`, and names a released module whose
  `go.mod` at that commit has the expected module path (with `/vN` from
  MAJOR 2 on);
- each module's tag history, replayed in creation order, follows the sequence
  and pre-release rules;
- every `go.mod` is either a released module or listed as unversioned;
- `CHANGELOG.md` exists for each released module and has a dated section for
  each tag, and only for tags;
- `require` lines between the modules name tagged versions;
- the two C headers agree on the ABI version, and the JVM binding expects its
  MAJOR; the client and the test server agree on the ALPN; protocol and
  manifest versions are positive integers; unreleased Gradle projects declare
  no version;
- with `--remote origin`, each local tag is the same object as the remote one
  (a moved or recreated tag is an error);
- with `--sumdb`, the module hash computed from each tag equals the record in
  `sum.golang.org` — content published under a version cannot be changed.

`check_versions.py release <tag>` additionally checks that the version is the
next one, is not on the remote and not in the checksum database, has its
release notes in the tagged commit, and that its `require` lines name tagged
versions. Exit code 1 means a violation, 2 that the check could not run.

The tests (`python3 -m unittest discover -s tools/versioning`) create throw-away
repositories with one deliberate violation each, and check the hash
computation against the published hashes of `v0.1.0`.
