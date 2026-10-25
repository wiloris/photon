#!/usr/bin/env python3
"""Release version checks for the Photon repository (VERSIONING.md).

The authoritative version of a released Go module is its Git tag
<module directory>/vX.Y.Z; no version number is written into the sources.
This script checks that tags, module metadata, release notes and the few
version constants that do live in the sources agree with each other, and that
a published version is never reused for different content.

    check_versions.py check [--remote origin] [--sumdb]
    check_versions.py release <tag> [--commit REV] [--remote origin] [--sumdb] [--pushed]

`check` validates the whole repository: every tag, the version history of
every module, the module metadata at HEAD. `release` is the gate before a tag
is pushed: the version must be new, the next one in sequence, and documented.
With --pushed it verifies, after the push, that the tag on the remote is the
one that was checked. Exit code 0 means no violations, 1 means violations,
2 means the check itself could not run.

Only the Python standard library is used: the script runs on a bare CI runner.
"""

import argparse
import base64
import hashlib
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request

MODULE_PREFIX = "github.com/wiloris/photon/"

# Go modules that are part of the repository but are not released and never
# get version tags (README.md, "Go modules"). Every go.mod in the tree must be
# either a released module or listed here: a new module needs a decision.
UNVERSIONED_MODULES = {
    "photon-core/tests/transportlab/bridgesim",
}

# Gradle projects. None of them is published (photon-sdk/build.gradle.kts:
# Maven coordinates and publication are deferred), so none may declare a
# version: a version that nothing publishes is a second, unchecked source.
UNRELEASED_GRADLE_PROJECTS = [
    "photon-sdk",
    "photon-devtools-core",
    "photon-developer-tools",
]

PRERELEASE_STAGES = ("alpha", "beta", "rc")

SEMVER_RE = re.compile(
    r"^v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-(alpha|beta|rc)\.([1-9]\d*))?$")

CHANGELOG = "CHANGELOG.md"
CHANGELOG_HEADING_RE = re.compile(r"^## (\S+)(.*)$")
CHANGELOG_DATED_RE = re.compile(r"^ — \d{4}-\d{2}-\d{2}$")

DEFAULT_SUMDB = "https://sum.golang.org"


class CheckError(Exception):
    """The check could not be performed (as opposed to a violation)."""


# --------------------------------------------------------------------------
# versions


class Version:
    def __init__(self, text):
        m = SEMVER_RE.match(text)
        if not m:
            raise ValueError(text)
        self.text = text
        self.major, self.minor, self.patch = (int(m.group(i)) for i in (1, 2, 3))
        self.stage = m.group(4)
        self.number = int(m.group(5)) if m.group(5) else 0

    @property
    def base(self):
        return (self.major, self.minor, self.patch)

    @property
    def prerelease(self):
        return self.stage is not None

    def key(self):
        # A final release sorts after every pre-release of the same base.
        if self.stage is None:
            return self.base + (len(PRERELEASE_STAGES), 0)
        return self.base + (PRERELEASE_STAGES.index(self.stage), self.number)

    def __repr__(self):
        return self.text


def parse_version(text):
    try:
        return Version(text)
    except ValueError:
        return None


def fmt_base(base):
    return "v%d.%d.%d" % base


def next_version_problem(existing, new):
    """Return None if `new` may follow `existing` (same module), else why not.

    The rules (VERSIONING.md, "Sequence"): a version is never reused; a
    release base is the next PATCH of an existing MINOR line, the next MINOR
    of an existing MAJOR, or the next MAJOR — no gaps; pre-releases of a base
    come before its final release, stages go alpha → beta → rc, and numbers
    within a stage go 1, 2, 3 without gaps.
    """
    texts = {v.text for v in existing}
    if new.text in texts:
        return "version %s is already used" % new.text

    finals = [v for v in existing if not v.prerelease]
    final_bases = {v.base for v in finals}
    same_base = [v for v in existing if v.base == new.base]

    if new.base in final_bases:
        return ("%s is already released; a pre-release or a repeat of it "
                "would sort below the published version" % fmt_base(new.base))

    if not existing:
        if new.base not in ((0, 1, 0), (1, 0, 0)):
            return ("the first version of a module is v0.1.0 or v1.0.0 "
                    "(or a pre-release of them), not %s" % new.text)
    elif not same_base:
        allowed = allowed_bases(finals, existing)
        if new.base not in allowed:
            return ("%s does not follow the released versions: allowed next "
                    "bases are %s" % (new.text, ", ".join(
                        fmt_base(b) for b in sorted(allowed)) or "none"))

    if new.prerelease:
        stage_index = PRERELEASE_STAGES.index(new.stage)
        same_stage = [v.number for v in same_base if v.stage == new.stage]
        later = [v for v in same_base
                 if PRERELEASE_STAGES.index(v.stage) > stage_index]
        if later:
            return ("%s goes back from %s to %s" % (
                new.text, max(later, key=Version.key).text, new.stage))
        expected = max(same_stage) + 1 if same_stage else 1
        if new.number != expected:
            return ("%s skips pre-release numbers: the next %s of %s is "
                    "%s.%d" % (new.text, new.stage, fmt_base(new.base),
                               new.stage, expected))
    return None


def allowed_bases(finals, existing):
    """Bases a new version line may start from, given the released finals."""
    allowed = set()
    # A base that has pre-releases but no final yet is still open.
    pending = {v.base for v in existing} - {v.base for v in finals}
    allowed |= pending
    if not finals:
        # Only pre-releases so far: nothing else may start before one is final.
        return allowed
    lines = {}
    for v in finals:
        lines.setdefault((v.major, v.minor), []).append(v.patch)
    for (major, minor), patches in lines.items():
        allowed.add((major, minor, max(patches) + 1))
    majors = {}
    for major, minor in lines:
        majors[major] = max(majors.get(major, -1), minor)
    for major, minor in majors.items():
        allowed.add((major, minor + 1, 0))
    allowed.add((max(majors) + 1, 0, 0))
    # A new line whose base already has a pre-release is covered by `pending`;
    # a base that is already final is never allowed again.
    return allowed - {v.base for v in finals}


# --------------------------------------------------------------------------
# git


class Git:
    def __init__(self, root):
        self.root = root

    def run(self, *args, check=True, binary=False):
        p = subprocess.run(["git", "-C", self.root] + list(args),
                           capture_output=True)
        if check and p.returncode != 0:
            raise CheckError("git %s: %s" % (" ".join(args),
                                             p.stderr.decode().strip()))
        return p.stdout if binary else p.stdout.decode()

    def ok(self, *args):
        return subprocess.run(["git", "-C", self.root] + list(args),
                              capture_output=True).returncode == 0

    def rev(self, rev):
        return self.run("rev-parse", "--verify", "--quiet", rev + "^{commit}").strip()

    def tags(self):
        """name → (object sha, object type, commit sha, subject, date)."""
        fmt = "%(refname)%00%(objectname)%00%(objecttype)%00%(*objectname)%00" \
              "%(contents:subject)%00%(creatordate:unix)"
        out = self.run("for-each-ref", "--format=" + fmt, "refs/tags")
        tags = {}
        for line in out.splitlines():
            ref, obj, typ, peeled, subject, date = line.split("\0")
            name = ref[len("refs/tags/"):]
            commit = peeled if typ == "tag" else obj
            tags[name] = (obj, typ, commit, subject, int(date or 0))
        return tags

    def file(self, rev, path):
        p = subprocess.run(["git", "-C", self.root, "show", "%s:%s" % (rev, path)],
                           capture_output=True)
        return p.stdout.decode() if p.returncode == 0 else None

    def go_mod_dirs(self, rev):
        out = self.run("ls-tree", "-r", "--name-only", "--full-tree", rev)
        dirs = []
        for path in out.splitlines():
            if path == "go.mod" or path.endswith("/go.mod"):
                dirs.append(os.path.dirname(path))
        return sorted(dirs)

    def blobs(self, rev, directory):
        """[(mode, sha, path)] of every tree entry under directory at rev."""
        args = ["ls-tree", "-r", "-z", "--full-tree", rev]
        if directory:
            args += ["--", directory + "/"]
        out = self.run(*args)
        entries = []
        for rec in out.split("\0"):
            if not rec:
                continue
            meta, path = rec.split("\t", 1)
            mode, typ, sha = meta.split()
            entries.append((mode, typ, sha, path))
        return entries

    def blob_contents(self, shas):
        """sha → bytes, read in one `git cat-file --batch`."""
        if not shas:
            return {}
        p = subprocess.run(["git", "-C", self.root, "cat-file", "--batch"],
                           input="".join(s + "\n" for s in shas).encode(),
                           capture_output=True, check=True)
        data, out, pos = p.stdout, {}, 0
        for sha in shas:
            nl = data.index(b"\n", pos)
            header = data[pos:nl].split()
            size = int(header[2])
            out[sha] = data[nl + 1:nl + 1 + size]
            pos = nl + 1 + size + 1
        return out

    def remote_tags(self, remote):
        out = self.run("ls-remote", "--tags", remote)
        tags = {}
        for line in out.splitlines():
            sha, ref = line.split("\t")
            if ref.endswith("^{}"):
                continue
            tags[ref[len("refs/tags/"):]] = sha
        return tags


# --------------------------------------------------------------------------
# module metadata


def module_path(go_mod_text):
    if go_mod_text is None:
        return None
    m = re.search(r"^module\s+(\S+)", go_mod_text, re.M)
    return m.group(1).strip('"') if m else None


def internal_requires(go_mod_text):
    """[(module path, version)] of requirements on modules of this repository."""
    reqs = []
    in_block = False
    for raw in go_mod_text.splitlines():
        line = raw.split("//")[0].strip()
        if in_block:
            if line == ")":
                in_block = False
                continue
            parts = line.split()
        elif line.startswith("require ("):
            in_block = True
            continue
        elif line.startswith("require "):
            parts = line.split()[1:]
        else:
            continue
        if len(parts) >= 2 and parts[0].startswith(MODULE_PREFIX):
            reqs.append((parts[0], parts[1]))
    return reqs


def expected_module_path(directory, version):
    path = MODULE_PREFIX + directory
    if version is not None and version.major >= 2:
        path += "/v%d" % version.major
    return path


def changelog_sections(text):
    """version → (dated?, body text) for every `## <version>` heading."""
    sections, current, dated, body = {}, None, False, []
    for line in text.splitlines():
        m = CHANGELOG_HEADING_RE.match(line)
        if m:
            if current is not None:
                sections[current] = (dated, "\n".join(body).strip())
            current, dated, body = m.group(1), bool(CHANGELOG_DATED_RE.match(m.group(2))), []
        elif current is not None:
            body.append(line)
    if current is not None:
        sections[current] = (dated, "\n".join(body).strip())
    return sections


# --------------------------------------------------------------------------
# module hash (the h1: value of go.sum and sum.golang.org)


def module_files(git, rev, directory, all_mod_dirs):
    """Files of the module zip the go command builds from the tag.

    The rules of golang.org/x/mod/zip and cmd/go/internal/modfetch: files
    under the module directory, without nested modules, vendor directories,
    VCS directories and non-regular files; a module in a subdirectory
    without its own LICENSE gets the LICENSE of the repository root.
    """
    nested = [d for d in all_mod_dirs
              if d != directory and (directory == "" or d.startswith(directory + "/"))]
    prefix = directory + "/" if directory else ""
    files = {}
    for mode, typ, sha, path in git.blobs(rev, directory):
        if typ != "blob" or mode not in ("100644", "100755"):
            continue
        if any(path.startswith(n + "/") for n in nested):
            continue
        rel = path[len(prefix):]
        parts = rel.split("/")
        if any(p in (".git", ".hg", ".svn", ".bzr") for p in parts[:-1]):
            continue
        if "vendor" in parts[:-1]:
            continue
        files[rel] = sha
    if directory and "LICENSE" not in files:
        for mode, typ, sha, path in git.blobs(rev, ""):
            if path == "LICENSE" and typ == "blob":
                files["LICENSE"] = sha
    return files


def hash1(named_contents):
    lines = []
    for name in sorted(named_contents):
        lines.append("%s  %s\n" % (hashlib.sha256(named_contents[name]).hexdigest(), name))
    return "h1:" + base64.b64encode(hashlib.sha256("".join(lines).encode()).digest()).decode()


def module_hashes(git, rev, directory, mod_path, version, all_mod_dirs):
    files = module_files(git, rev, directory, all_mod_dirs)
    contents = git.blob_contents(sorted(set(files.values())))
    prefix = "%s@%s/" % (mod_path, version)
    zip_hash = hash1({prefix + rel: contents[sha] for rel, sha in files.items()})
    mod_hash = hash1({"go.mod": contents[files["go.mod"]]})
    return zip_hash, mod_hash


def sumdb_lookup(base_url, mod_path, version):
    """(zip hash, go.mod hash), or None if the checksum database has no record."""
    url = "%s/lookup/%s@%s" % (base_url, escape_module_path(mod_path), version)
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            text = resp.read().decode()
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        if e.code in (404, 410):
            return None
        raise CheckError("sumdb %s: HTTP %d %s" % (url, e.code, body.strip()))
    except (urllib.error.URLError, OSError) as e:
        raise CheckError("sumdb %s: %s" % (url, e))
    zip_hash = mod_hash = None
    for line in text.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[0] == mod_path and parts[1] == version:
            zip_hash = parts[2]
        elif len(parts) == 3 and parts[0] == mod_path and parts[1] == version + "/go.mod":
            mod_hash = parts[2]
    if zip_hash is None or mod_hash is None:
        raise CheckError("sumdb %s: unexpected answer" % url)
    return zip_hash, mod_hash


def escape_module_path(path):
    # Module proxy escaping: an upper-case letter becomes '!' + lower case.
    return re.sub(r"[A-Z]", lambda m: "!" + m.group(0).lower(), path)


# --------------------------------------------------------------------------
# checks


class Checker:
    def __init__(self, root, main_ref=None, remote=None, sumdb=None,
                 sumdb_lookup=sumdb_lookup):
        self.git = Git(root)
        self.root = root
        self.remote = remote
        self.sumdb = sumdb
        self.sumdb_lookup = sumdb_lookup
        self.errors = []
        self.notes = []
        self.main_ref = main_ref or self._default_main()
        self._tags = None
        self._remote_tags = None

    def _default_main(self):
        for ref in ("origin/main", "main"):
            if self.git.ok("rev-parse", "--verify", "--quiet", ref + "^{commit}"):
                return ref
        raise CheckError("neither origin/main nor main exists; pass --main-ref")

    def error(self, msg):
        self.errors.append(msg)

    def note(self, msg):
        self.notes.append(msg)

    # ---- inventory

    def released_dirs(self, rev):
        dirs = []
        for d in self.git.go_mod_dirs(rev):
            if d in UNVERSIONED_MODULES:
                continue
            dirs.append(d)
        return dirs

    def tags(self):
        if self._tags is None:
            self._tags = self.git.tags()
        return self._tags

    def remote_tags(self):
        if self._remote_tags is None:
            self._remote_tags = self.git.remote_tags(self.remote)
        return self._remote_tags

    def split_tag(self, name):
        """(module directory, Version) or None if the name is not <dir>/vX.Y.Z."""
        directory, _, ver = name.rpartition("/")
        if not directory:
            return None, parse_version(ver)
        return directory, parse_version(ver)

    def module_versions(self, exclude=None):
        """directory → [(Version, tag name, date)] for every well-formed tag."""
        versions = {}
        for name, (_, _, _, _, date) in self.tags().items():
            if name == exclude:
                continue
            directory, ver = self.split_tag(name)
            if directory and ver:
                versions.setdefault(directory, []).append((ver, name, date))
        return versions

    # ---- tag checks (one tag)

    def check_tag(self, name, info):
        obj, typ, commit, subject, _ = info
        directory, ver = self.split_tag(name)
        if ver is None:
            raw = name.rpartition("/")[2]
            self.error("tag %s: %r is not vMAJOR.MINOR.PATCH with an optional "
                       "-alpha.N/-beta.N/-rc.N suffix" % (name, raw))
            return
        if not directory:
            self.error("tag %s: a version tag must be prefixed with the module "
                       "directory (<dir>/%s); the repository has no root module"
                       % (name, ver.text))
            return
        if directory in UNVERSIONED_MODULES:
            self.error("tag %s: %s is not a released module and must not be "
                       "tagged" % (name, directory))
            return
        if typ != "tag":
            self.error("tag %s: lightweight tag; release tags are annotated "
                       "(git tag -a) so that they carry who, when and what" % name)
        elif subject != "%s %s" % (directory, ver.text):
            self.error("tag %s: message %r, expected %r" % (
                name, subject, "%s %s" % (directory, ver.text)))
        go_mod = self.git.file(commit, directory + "/go.mod")
        if go_mod is None:
            self.error("tag %s: %s/go.mod does not exist at %s — the tag does "
                       "not name a module" % (name, directory, commit[:12]))
            return
        got, want = module_path(go_mod), expected_module_path(directory, ver)
        if got != want:
            self.error("tag %s: module path at %s is %s, expected %s" % (
                name, commit[:12], got, want))
        if not self.git.ok("merge-base", "--is-ancestor", commit, self.main_ref):
            self.error("tag %s: commit %s is not on %s; releases are cut from "
                       "the main history" % (name, commit[:12], self.main_ref))

    # ---- whole repository

    def check_repository(self):
        head = "HEAD"
        all_dirs = self.git.go_mod_dirs(head)
        for d in all_dirs:
            path = module_path(self.git.file(head, (d + "/" if d else "") + "go.mod"))
            if d in UNVERSIONED_MODULES:
                continue
            if not d:
                self.error("go.mod at the repository root: a root module is not "
                           "part of the release scheme (VERSIONING.md)")
                continue
            if path is None or not (path == MODULE_PREFIX + d or
                                    re.fullmatch(re.escape(MODULE_PREFIX + d) + r"/v([2-9]|[1-9]\d+)", path)):
                self.error("%s/go.mod: module path %s does not match the "
                           "directory; every go.mod is either a released module "
                           "%s<dir> or listed in UNVERSIONED_MODULES" % (
                               d, path, MODULE_PREFIX))

        tags = self.tags()
        for name in sorted(tags):
            self.check_tag(name, tags[name])

        released = set(self.released_dirs(head))
        versions = self.module_versions()
        for directory, entries in sorted(versions.items()):
            self.check_history(directory, entries)
        for directory in sorted(released):
            self.check_head_module(directory, versions.get(directory, []))
        self.check_requires(head, versions)
        self.check_source_versions(head)
        if self.remote:
            self.check_remote()
        if self.sumdb:
            self.check_sumdb(all_dirs)

    def check_history(self, directory, entries):
        # Replay in creation order: each version must have been a valid next
        # version when it was tagged.
        seen = []
        for ver, name, _ in sorted(entries, key=lambda e: (e[2], e[0].key())):
            problem = next_version_problem(seen, ver)
            if problem:
                self.error("tag %s: %s" % (name, problem))
            seen.append(ver)

    def check_head_module(self, directory, entries):
        text = self.git.file("HEAD", directory + "/" + CHANGELOG)
        if text is None:
            self.error("%s/%s is missing: release notes of a module live next "
                       "to it" % (directory, CHANGELOG))
            return
        sections = changelog_sections(text)
        for heading in sections:
            if heading != "Unreleased" and parse_version(heading) is None:
                self.error("%s/%s: heading %r is neither 'Unreleased' nor a "
                           "version" % (directory, CHANGELOG, heading))
        tagged = {ver.text for ver, _, _ in entries}
        for ver, name, _ in entries:
            sec = sections.get(ver.text)
            if sec is None:
                self.error("%s/%s: no section for the released %s (tag %s)" % (
                    directory, CHANGELOG, ver.text, name))
            elif not sec[0] or not sec[1]:
                self.error("%s/%s: section %s needs a date ('## %s — YYYY-MM-DD') "
                           "and a non-empty body" % (directory, CHANGELOG, ver.text, ver.text))
        for heading in sections:
            if parse_version(heading) and heading not in tagged:
                self.error("%s/%s: section %s describes a version that has no "
                           "tag; unreleased changes go under '## Unreleased'" % (
                               directory, CHANGELOG, heading))

    def check_requires(self, rev, versions):
        for directory in self.git.go_mod_dirs(rev):
            text = self.git.file(rev, (directory + "/" if directory else "") + "go.mod")
            for dep, ver in internal_requires(text):
                dep_dir = dep[len(MODULE_PREFIX):]
                dep_dir = re.sub(r"/v[2-9]\d*$|/v[1-9]\d+$", "", dep_dir)
                known = {v.text for v, _, _ in versions.get(dep_dir, [])}
                if ver not in known:
                    self.error("%s/go.mod requires %s %s, which is not a tagged "
                               "version of %s (tagged: %s); a released module may "
                               "depend only on released versions" % (
                                   directory, dep, ver, dep_dir,
                                   ", ".join(sorted(known)) or "none"))

    def check_source_versions(self, rev):
        """Version constants that do live in the sources must agree."""
        abi = {}
        for path in ("photon-bindings/c/photon_core.h",
                     "photon-bindings/c/internal_cgo_types.h"):
            text = self.git.file(rev, path)
            if text is None:
                continue
            got = tuple(
                int(m.group(1)) if m else None for m in (
                    re.search(r"^#define PHOTON_ABI_MAJOR (\d+)\s*$", text, re.M),
                    re.search(r"^#define PHOTON_ABI_MINOR (\d+)\s*$", text, re.M)))
            if None in got:
                self.error("%s: PHOTON_ABI_MAJOR/PHOTON_ABI_MINOR not found" % path)
                continue
            abi[path] = got
        if len(set(abi.values())) > 1:
            self.error("C ABI version differs between headers: %s" % ", ".join(
                "%s=%d.%d" % (p, a[0], a[1]) for p, a in sorted(abi.items())))
        kt = self.git.file(rev, "photon-bindings/jvm/kotlin/Photon.kt")
        if kt is not None and abi:
            m = re.search(r"const val EXPECTED_ABI_MAJOR = (\d+)", kt)
            major = next(iter(abi.values()))[0]
            if not m:
                self.error("photon-bindings/jvm/kotlin/Photon.kt: EXPECTED_ABI_MAJOR not found")
            elif int(m.group(1)) != major:
                self.error("photon-bindings/jvm/kotlin/Photon.kt: EXPECTED_ABI_MAJOR = %s, "
                           "the C ABI is %d.x" % (m.group(1), major))

        alpn = {}
        for path, pattern in (
                ("photon-core/adapters/quic/quic.go", r'const defaultALPN = "([^"]+)"'),
                ("photon-core/tests/transportlab/bridgesim/main.go", r'NextProtos:\s*\[\]string\{"([^"]+)"\}')):
            text = self.git.file(rev, path)
            if text is None:
                continue
            m = re.search(pattern, text)
            if not m:
                self.error("%s: ALPN not found" % path)
            else:
                alpn[path] = m.group(1)
        if len(set(alpn.values())) > 1:
            self.error("ALPN differs between client and test server: %s" % alpn)

        for path, const in (
                ("photon-core/services/session/codec.go", "ProtocolVersion"),
                ("photon-plugin-runtime/manifest/types.go", "SupportedProtocolVersion"),
                ("photon-plugin-runtime/manifest/types.go", "SupportedManifestVersion")):
            text = self.git.file(rev, path)
            if text is None:
                continue
            m = re.search(r"^const %s = (\d+)\s*$" % const, text, re.M)
            if not m or int(m.group(1)) < 1:
                self.error("%s: %s must be a positive integer constant" % (path, const))

        for project in UNRELEASED_GRADLE_PROJECTS:
            for name in ("build.gradle.kts", "gradle.properties"):
                text = self.git.file(rev, "%s/%s" % (project, name))
                if text is None:
                    continue
                if re.search(r"^\s*version\s*=", text, re.M):
                    self.error("%s/%s declares a version, but %s is not a released "
                               "component (VERSIONING.md)" % (project, name, project))

    def check_remote(self):
        local, remote = self.tags(), self.remote_tags()
        for name, sha in sorted(remote.items()):
            if name not in local:
                self.error("tag %s exists on %s but not locally; fetch the tags "
                           "(git fetch --tags) before checking" % (name, self.remote))
            elif local[name][0] != sha:
                self.error("tag %s: %s has object %s, the local tag is %s — a "
                           "published tag was moved or recreated" % (
                               name, self.remote, sha[:12], local[name][0][:12]))
        for name in sorted(set(local) - set(remote)):
            self.note("tag %s is not on %s yet" % (name, self.remote))

    def check_sumdb(self, all_dirs):
        for name, (_, _, commit, _, _) in sorted(self.tags().items()):
            directory, ver = self.split_tag(name)
            if not directory or ver is None or directory in UNVERSIONED_MODULES:
                continue
            self.compare_sumdb(name, directory, ver, commit, all_dirs, published_expected=True)

    def compare_sumdb(self, name, directory, ver, commit, all_dirs, published_expected):
        mod_path = expected_module_path(directory, ver)
        if self.git.file(commit, directory + "/go.mod") is None:
            return None
        local = module_hashes(self.git, commit, directory, mod_path, ver.text,
                              self.git.go_mod_dirs(commit))
        record = self.sumdb_lookup(self.sumdb, mod_path, ver.text)
        if record is None:
            if published_expected:
                self.note("%s@%s: no record in the checksum database yet" % (mod_path, ver.text))
            return None
        if record != local:
            self.error("%s@%s: checksum database has %s %s, tag %s gives %s %s — "
                       "the version was published with different content and "
                       "can never be reused" % (mod_path, ver.text, record[0],
                                                record[1], name, local[0], local[1]))
        return record

    # ---- release gate

    def check_release(self, name, commit_rev, pushed):
        directory, ver = self.split_tag(name)
        tags = self.tags()
        if name in tags:
            commit = tags[name][2]
            self.check_tag(name, tags[name])
            if commit_rev and self.git.rev(commit_rev) != commit:
                self.error("tag %s points at %s, not at %s" % (
                    name, commit[:12], commit_rev))
        else:
            if pushed:
                self.error("tag %s does not exist locally" % name)
                return
            commit = self.git.rev(commit_rev or "HEAD")
            if not commit:
                raise CheckError("cannot resolve %s" % commit_rev)
            # Validate the tag as if it were created on `commit`.
            self.check_tag(name, ("", "tag", commit, "%s %s" % (directory, ver.text) if ver else "", 0))
        if ver is None or not directory or directory in UNVERSIONED_MODULES:
            return

        versions = self.module_versions(exclude=name)
        problem = next_version_problem([v for v, _, _ in versions.get(directory, [])], ver)
        if problem:
            self.error("release %s: %s" % (name, problem))

        text = self.git.file(commit, directory + "/" + CHANGELOG)
        sec = changelog_sections(text).get(ver.text) if text else None
        if sec is None or not sec[0] or not sec[1]:
            self.error("release %s: %s/%s at %s has no dated, non-empty section "
                       "'## %s — YYYY-MM-DD'" % (name, directory, CHANGELOG,
                                                 commit[:12], ver.text))

        if self.git.file(commit, directory + "/go.mod") is not None:
            known = {d: list(v) for d, v in versions.items()}
            known.setdefault(directory, []).append((ver, name, 0))
            text = self.git.file(commit, directory + "/go.mod")
            for dep, dver in internal_requires(text):
                dep_dir = re.sub(r"/v\d+$", "", dep[len(MODULE_PREFIX):])
                if dver not in {v.text for v, _, _ in known.get(dep_dir, [])}:
                    self.error("release %s: %s/go.mod requires %s %s, which is not "
                               "tagged; tag the dependency first" % (
                                   name, directory, dep, dver))

        if self.remote:
            remote = self.remote_tags()
            if pushed:
                if name not in remote:
                    self.error("tag %s is not on %s" % (name, self.remote))
                elif name in tags and remote[name] != tags[name][0]:
                    self.error("tag %s on %s is %s, the checked local tag is %s" % (
                        name, self.remote, remote[name][:12], tags[name][0][:12]))
            elif name in remote:
                self.error("release %s: the tag already exists on %s — published "
                           "versions are immutable; release the next version" % (
                               name, self.remote))
        if self.sumdb:
            record = self.compare_sumdb(name, directory, ver, commit,
                                        self.git.go_mod_dirs(commit),
                                        published_expected=pushed)
            if record is not None and not pushed:
                self.error("release %s: %s@%s is already in the checksum "
                           "database — this version is published" % (
                               name, expected_module_path(directory, ver), ver.text))

    def report(self, out=None):
        out = out or sys.stdout
        for n in self.notes:
            print("note: " + n, file=out)
        for e in self.errors:
            print("error: " + e, file=out)
        print("%d violation(s)" % len(self.errors), file=out)


def print_inventory(checker, out=None):
    out = out or sys.stdout
    versions = checker.module_versions()
    print("released modules (version = Git tag <dir>/vX.Y.Z):", file=out)
    for d in checker.released_dirs("HEAD"):
        vs = sorted((v for v, _, _ in versions.get(d, [])), key=Version.key)
        print("  %-28s %s" % (d, ", ".join(v.text for v in vs) or "(no release)"), file=out)
    print("unversioned modules: %s" % ", ".join(sorted(UNVERSIONED_MODULES)), file=out)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--repo", default=os.getcwd(), help="repository root")
    ap.add_argument("--main-ref", help="branch releases must be on (default origin/main, else main)")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("check", "release"):
        p = sub.add_parser(name)
        p.add_argument("--remote", help="compare tags with this remote (git ls-remote)")
        p.add_argument("--sumdb", nargs="?", const=DEFAULT_SUMDB,
                       help="compare module hashes with the checksum database "
                            "(default %s)" % DEFAULT_SUMDB)
        if name == "release":
            p.add_argument("tag")
            p.add_argument("--commit", help="commit the tag is or will be on (default HEAD)")
            p.add_argument("--pushed", action="store_true",
                           help="verify a tag that has already been pushed")
    args = ap.parse_args(argv)
    root = Git(args.repo).run("rev-parse", "--show-toplevel").strip()
    try:
        c = Checker(root, main_ref=args.main_ref, remote=args.remote, sumdb=args.sumdb)
        if args.cmd == "check":
            c.check_repository()
            print_inventory(c)
        else:
            c.check_release(args.tag, args.commit, args.pushed)
    except CheckError as e:
        print("cannot check: %s" % e, file=sys.stderr)
        return 2
    c.report()
    return 1 if c.errors else 0


if __name__ == "__main__":
    sys.exit(main())
