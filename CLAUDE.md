# CLAUDE.md

Standing instructions for Claude Code in this repository. They apply to every
task, not only to release work.

## Versioning — before every push

The release contract is `VERSIONING.md`; the checker is
`tools/versioning/check_versions.py`. Before any `git push`:

1. **Find the affected components.** From the diff since the last pushed
   commit, list which released modules changed (`photon-core`,
   `photon-plugin-sdk`, `photon-plugin-runtime`, `photon-bindings/c`), and
   whether a change touches a boundary with its own version: the C ABI
   (`photon_core.h`), the client ↔ server session protocol
   (`ProtocolVersion`, ALPN), the plugin IPC protocol or the manifest format.
2. **Decide, per affected component, whether a version bump is due**, and
   state the reason in SemVer terms of `VERSIONING.md` §2: before `v1.0.0`,
   `0.y.(z+1)` for backward-compatible fixes only, `0.(y+1).0` for anything
   else; an incompatible change is named as such.
3. **A bump is a release decision, not a per-commit action.** Several commits
   may go into one release. If no release is intended, write the change under
   `## Unreleased` in the module's `CHANGELOG.md` and do not tag.
4. **When releasing**, move the notes to `## vX.Y.Z — YYYY-MM-DD`, move
   `require` lines of dependent modules only to tagged versions (dependencies
   are released first), and never write a module version into sources or
   build files — the version is the tag.
5. **Check compatibility explicitly**: exported Go API of the changed modules,
   the C ABI (an incompatible change needs a new `PHOTON_ABI_MAJOR`), the
   protocol versions (an incompatible wire change needs a new protocol version
   or a specified migration, VERSIONING.md §3), and Go module paths (`/vN` from
   MAJOR 2). Component numbers never prove compatibility.
6. **Run the checks** and the tests of the changed modules:
   ```sh
   python3 -m unittest discover -s tools/versioning
   python3 tools/versioning/check_versions.py check --remote origin --sumdb
   # only when a tag is to be created:
   python3 tools/versioning/check_versions.py release <dir>/vX.Y.Z --remote origin --sumdb
   ```
   Do not push with a violation.
7. **Never change what is published**: no moving, deleting or recreating tags,
   no replacing release artifacts, no re-releasing a used version, no
   rewriting history of `main`.
8. **No release without a version change**, and no version change without a
   reason from step 2.
9. **After the checks pass**, prepare the annotated tag
   (`git tag -a <dir>/vX.Y.Z -m "<dir> vX.Y.Z"`) and the GitHub Release text
   (the changelog section). Pushing tags and publishing releases happen only
   as an action the user has explicitly authorised.
