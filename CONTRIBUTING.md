# Contributing to Photon

Thank you for your interest in Photon. This document explains how
contributions are accepted.

## Contributor License Agreement

Before a pull request can be merged, its author must accept the
[Photon Contributor License Agreement](CLA.md). In short: you keep the
copyright in your contribution and grant the maintainer (wiloris) a
non-exclusive license to use it in Photon and in other products, including
proprietary ones such as the ENGRAM application. Please read the full text —
it is a draft that has not yet been reviewed by a lawyer.

**How acceptance is recorded.** Linking to the CLA from this file does not, by
itself, mean that anyone has agreed to it. Acceptance is recorded explicitly:

1. In your pull request, post a comment with exactly this sentence:

   > I have read the Photon CLA (CLA.md) and I accept it for all my contributions to this repository.

2. The maintainer checks that the comment was posted by the GitHub account
   that authored the commits and records the acceptance (account, date, link
   to the comment) before merging.
3. A pull request with commits from several authors is merged only after every
   author has accepted the CLA.

Automating this step (for example, a CLA bot that checks the comment and keeps
the list of signatures) requires a GitHub App or a GitHub Action with its own
credentials. It is not set up yet; until then acceptance is checked manually.

## Pull requests

- Open an issue first for larger changes, so the design can be agreed before
  the work starts.
- Keep a pull request focused on one change.
- Every Go module you touch must pass, from its own directory:
  `go build ./...`, `go vet ./...`, `gofmt -l .` (no output) and `go test ./...`.
  JVM modules: `gradle test`.
- Add tests for the behaviour you change. Tests should fail when the rule they
  check is broken — a test that always passes is worse than no test.
- Do not change the frozen contracts without a prior discussion: the core ports
  (`photon-core/ports`), the C ABI (`photon-bindings/c/photon_core.h`, ABI 1.0)
  and action numbers in `photon-core/protobuf/photon.proto`.
- `photon-plugin-sdk` must keep zero external dependencies.
- Do not commit secrets, credentials, private keys, local paths or addresses of
  private infrastructure.

## License

By contributing you agree that your contribution is published in Photon under
the GNU AGPL-3.0-only license (see [LICENSE](LICENSE) and [NOTICE](NOTICE)) and
licensed to the maintainer as described in the [CLA](CLA.md).

## Security issues

Please do not report vulnerabilities in public issues — see
[SECURITY.md](SECURITY.md).
