"""Tests of check_versions.py.

Every rule is exercised on a throw-away Git repository in a temporary
directory, first in a state that passes and then with one deliberate
violation, so a rule that never fires cannot pass unnoticed. Nothing here
touches the real repository except TestRealRepository, which only reads it.

    python3 -m unittest discover -s tools/versioning -v
"""

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_versions as cv  # noqa: E402

P = cv.MODULE_PREFIX
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def changelog(*versions):
    text = "# Changelog\n\n## Unreleased\n"
    for v in versions:
        text += "\n## %s — 2026-10-24\n\nNotes for %s.\n" % (v, v)
    return text


class Repo:
    """A minimal two-module repository shaped like Photon."""

    def __init__(self, tmp):
        self.dir = os.path.join(tmp, "repo")
        os.makedirs(self.dir)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.email", "t@example.net")
        self.git("config", "user.name", "t")
        self.git("config", "tag.gpgSign", "false")
        self.git("config", "commit.gpgSign", "false")
        self.clock = 1700000000
        self.write("LICENSE", "license\n")
        self.write("core/go.mod", "module %score\n\ngo 1.22\n" % P)
        self.write("core/core.go", "package core\n")
        self.write("core/CHANGELOG.md", changelog())
        self.write("rt/go.mod", "module %srt\n\ngo 1.22\n" % P)
        self.write("rt/CHANGELOG.md", changelog())
        self.commit("init")

    def git(self, *args, env=None):
        e = dict(os.environ)
        if env:
            e.update(env)
        return subprocess.run(["git", "-C", self.dir] + list(args), check=True,
                              capture_output=True, env=e).stdout.decode().strip()

    def write(self, path, text):
        full = os.path.join(self.dir, path)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w") as f:
            f.write(text)

    def remove(self, path):
        os.remove(os.path.join(self.dir, path))

    def _env(self):
        self.clock += 60
        d = "%d +0000" % self.clock
        return {"GIT_AUTHOR_DATE": d, "GIT_COMMITTER_DATE": d}

    def commit(self, msg):
        self.git("add", "-A")
        self.git("commit", "-q", "--allow-empty", "-m", msg, env=self._env())
        return self.git("rev-parse", "HEAD")

    def tag(self, name, message=None, rev="HEAD", lightweight=False):
        if lightweight:
            self.git("tag", name, rev)
            return
        d, _, v = name.rpartition("/")
        self.git("tag", "-a", name, "-m", message or "%s %s" % (d, v), rev, env=self._env())

    def release(self, directory, *versions):
        """Tag versions of one module in order, documenting each first."""
        for v in versions:
            done = [t.rpartition("/")[2] for t in self.git("tag", "-l", directory + "/v*").split()]
            self.write(directory + "/CHANGELOG.md", changelog(*(done + [v])))
            self.commit("release %s %s" % (directory, v))
            self.tag("%s/%s" % (directory, v))

    def checker(self, **kw):
        return cv.Checker(self.dir, main_ref="main", **kw)


class Base(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp = self._tmp.name
        self.repo = Repo(self.tmp)

    def tearDown(self):
        self._tmp.cleanup()

    def check(self, **kw):
        c = self.repo.checker(**kw)
        c.check_repository()
        return c

    def release(self, tag, commit=None, pushed=False, **kw):
        c = self.repo.checker(**kw)
        c.check_release(tag, commit, pushed)
        return c

    def assertClean(self, c):
        self.assertEqual(c.errors, [])

    def assertViolation(self, c, fragment):
        self.assertTrue(any(fragment in e for e in c.errors),
                        "no error containing %r in %r" % (fragment, c.errors))


class TestVersionRules(unittest.TestCase):
    def problem(self, existing, new):
        return cv.next_version_problem([cv.Version(v) for v in existing], cv.Version(new))

    def test_format(self):
        for good in ("v0.1.0", "v1.2.3", "v0.4.0-alpha.1", "v0.4.0-rc.12", "v10.0.0"):
            self.assertIsNotNone(cv.parse_version(good), good)
        for bad in ("0.1.0", "v0.1", "v01.0.0", "v0.1.0-pre1", "v0.1.0-rc",
                    "v0.1.0-rc.0", "v0.1.0-RC.1", "v0.1.0+build", "v0.1.0-rc.1.2",
                    "v0.1.0-gamma.1"):
            self.assertIsNone(cv.parse_version(bad), bad)

    def test_first_version(self):
        for ok in ("v0.1.0", "v1.0.0", "v0.1.0-alpha.1", "v1.0.0-rc.1"):
            self.assertIsNone(self.problem([], ok), ok)
        for bad in ("v0.2.0", "v0.0.1", "v2.0.0", "v0.1.0-rc.2"):
            self.assertIsNotNone(self.problem([], bad), bad)

    def test_sequential_bumps(self):
        released = ["v0.1.0"]
        for ok in ("v0.1.1", "v0.2.0", "v1.0.0", "v0.2.0-rc.1"):
            self.assertIsNone(self.problem(released, ok), ok)
        for bad in ("v0.1.2", "v0.3.0", "v2.0.0", "v1.1.0"):
            self.assertIn("allowed next", self.problem(released, bad), bad)

    def test_reuse_and_old_versions(self):
        self.assertIn("already used", self.problem(["v0.1.0"], "v0.1.0"))
        self.assertIn("already released", self.problem(["v0.1.0"], "v0.1.0-rc.1"))
        self.assertIn("already used", self.problem(["v0.1.0", "v0.2.0"], "v0.1.0"))

    def test_maintenance_line(self):
        # A patch of an older line stays possible after a newer MINOR.
        self.assertIsNone(self.problem(["v0.1.0", "v0.2.0"], "v0.1.1"))
        self.assertIsNone(self.problem(["v0.1.0", "v1.0.0"], "v0.2.0"))

    def test_prerelease_sequence(self):
        self.assertIsNone(self.problem(["v0.1.0", "v0.2.0-alpha.1"], "v0.2.0-alpha.2"))
        self.assertIsNone(self.problem(["v0.1.0", "v0.2.0-alpha.1"], "v0.2.0-beta.1"))
        self.assertIsNone(self.problem(["v0.1.0", "v0.2.0-rc.2"], "v0.2.0"))
        self.assertIn("skips", self.problem(["v0.1.0", "v0.2.0-rc.1"], "v0.2.0-rc.3"))
        self.assertIn("skips", self.problem(["v0.1.0", "v0.2.0-alpha.1"], "v0.2.0-beta.2"))
        self.assertIn("goes back", self.problem(["v0.1.0", "v0.2.0-rc.1"], "v0.2.0-beta.1"))
        # An open pre-release line does not unlock anything beyond it.
        self.assertIn("allowed next", self.problem(["v0.1.0", "v0.2.0-rc.1"], "v0.4.0"))


class TestHash(unittest.TestCase):
    def test_module_files_rules(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = Repo(tmp)
            r.write("core/nested/go.mod", "module other\n")
            r.write("core/nested/x.go", "package x\n")
            r.write("core/vendor/v.go", "package v\n")
            r.write("core/sub/a.go", "package sub\n")
            os.symlink("core.go", os.path.join(r.dir, "core/link.go"))
            r.commit("files")
            g = cv.Git(r.dir)
            files = cv.module_files(g, "HEAD", "core", g.go_mod_dirs("HEAD"))
            self.assertEqual(sorted(files), ["CHANGELOG.md", "LICENSE", "core.go",
                                             "go.mod", "sub/a.go"])


class TestRepositoryCheck(Base):
    def test_clean_history(self):
        self.repo.release("core", "v0.1.0", "v0.1.1", "v0.2.0-rc.1", "v0.2.0")
        self.repo.release("rt", "v0.1.0")
        self.assertClean(self.check())

    def test_bad_tag_format(self):
        self.repo.tag("core/v0.1")
        self.assertViolation(self.check(), "is not vMAJOR.MINOR.PATCH")

    def test_root_tag(self):
        self.repo.tag("v0.1.0", message="x v0.1.0")
        self.assertViolation(self.check(), "prefixed with the module directory")

    def test_tag_without_module(self):
        self.repo.tag("nosuch/v0.1.0")
        self.assertViolation(self.check(), "does not name a module")

    def test_unversioned_module_tag(self):
        name = sorted(cv.UNVERSIONED_MODULES)[0]
        self.repo.write(name + "/go.mod", "module %s%s\n" % (P, name))
        self.repo.commit("bridgesim")
        self.assertClean(self.check())
        self.repo.tag(name + "/v0.1.0")
        self.assertViolation(self.check(), "is not a released module")

    def test_unclassified_module(self):
        self.repo.write("tools/x/go.mod", "module example.com/x\n")
        self.repo.commit("x")
        self.assertViolation(self.check(), "does not match the directory")

    def test_lightweight_tag(self):
        self.repo.release("core", "v0.1.0")
        self.repo.git("tag", "-d", "core/v0.1.0")
        self.repo.tag("core/v0.1.0", lightweight=True)
        self.assertViolation(self.check(), "lightweight tag")

    def test_tag_message(self):
        self.repo.write("core/CHANGELOG.md", changelog("v0.1.0"))
        self.repo.commit("notes")
        self.repo.tag("core/v0.1.0", message="core version 0.1.0")
        self.assertViolation(self.check(), "message")

    def test_module_path_mismatch_at_tag(self):
        self.repo.write("core/go.mod", "module %swrong\n" % P)
        self.repo.write("core/CHANGELOG.md", changelog("v0.1.0"))
        self.repo.commit("wrong path")
        self.repo.tag("core/v0.1.0")
        self.assertViolation(self.check(), "module path at")

    def test_major_suffix(self):
        self.repo.release("core", "v0.1.0", "v1.0.0")
        self.repo.write("core/CHANGELOG.md", changelog("v0.1.0", "v1.0.0", "v2.0.0"))
        self.repo.commit("v2 without suffix")
        self.repo.tag("core/v2.0.0")
        self.assertViolation(self.check(), "expected %score/v2" % P)

    def test_tag_off_main(self):
        self.repo.git("checkout", "-q", "-b", "side")
        self.repo.release("core", "v0.1.0")
        self.repo.git("checkout", "-q", "main")
        self.assertViolation(self.check(), "is not on main")

    def test_history_gap(self):
        self.repo.release("core", "v0.1.0", "v0.3.0")
        self.assertViolation(self.check(), "tag core/v0.3.0: v0.3.0 does not follow")

    def test_prerelease_after_final(self):
        self.repo.release("core", "v0.1.0", "v0.1.0-rc.1")
        self.assertViolation(self.check(), "already released")

    def test_missing_changelog(self):
        self.repo.remove("rt/CHANGELOG.md")
        self.repo.commit("drop notes")
        self.assertViolation(self.check(), "rt/CHANGELOG.md is missing")

    def test_released_version_without_notes(self):
        self.repo.release("core", "v0.1.0")
        self.repo.write("core/CHANGELOG.md", changelog())
        self.repo.commit("lose notes")
        self.assertViolation(self.check(), "no section for the released v0.1.0")

    def test_undated_notes(self):
        self.repo.release("core", "v0.1.0")
        self.repo.write("core/CHANGELOG.md", "## Unreleased\n\n## v0.1.0\n\nx\n")
        self.repo.commit("undated")
        self.assertViolation(self.check(), "needs a date")

    def test_changelog_heading(self):
        self.repo.write("core/CHANGELOG.md", "## Unreleased\n\n## 0.1.0 — 2026-10-24\n\nx\n")
        self.repo.commit("heading without v")
        self.assertViolation(self.check(), "neither 'Unreleased' nor a version")

    def test_notes_for_untagged_version(self):
        self.repo.write("core/CHANGELOG.md", changelog("v0.1.0"))
        self.repo.commit("premature")
        self.assertViolation(self.check(), "has no tag")

    def test_require_unreleased_version(self):
        self.repo.release("core", "v0.1.0")
        self.repo.write("rt/go.mod", "module %srt\n\nrequire %score v0.2.0\n" % (P, P))
        self.repo.commit("bump")
        self.assertViolation(self.check(), "not a tagged version of core")
        self.repo.write("rt/go.mod", "module %srt\n\nrequire (\n\t%score v0.1.0\n)\n" % (P, P))
        self.repo.commit("fix")
        self.assertClean(self.check())

    def test_abi_headers_disagree(self):
        self.repo.write("photon-bindings/c/photon_core.h",
                        "#define PHOTON_ABI_MAJOR 1\n#define PHOTON_ABI_MINOR 0\n")
        self.repo.write("photon-bindings/c/internal_cgo_types.h",
                        "#define PHOTON_ABI_MAJOR 1\n#define PHOTON_ABI_MINOR 0\n")
        self.repo.write("photon-bindings/jvm/kotlin/Photon.kt", "const val EXPECTED_ABI_MAJOR = 1\n")
        self.repo.commit("abi")
        self.assertClean(self.check())
        self.repo.write("photon-bindings/c/internal_cgo_types.h",
                        "#define PHOTON_ABI_MAJOR 1\n#define PHOTON_ABI_MINOR 1\n")
        self.repo.commit("drift")
        self.assertViolation(self.check(), "C ABI version differs")
        self.repo.write("photon-bindings/c/internal_cgo_types.h",
                        "#define PHOTON_ABI_MAJOR 1\n#define PHOTON_ABI_MINOR 0\n")
        self.repo.write("photon-bindings/jvm/kotlin/Photon.kt", "const val EXPECTED_ABI_MAJOR = 2\n")
        self.repo.commit("jvm drift")
        self.assertViolation(self.check(), "EXPECTED_ABI_MAJOR = 2")

    def test_alpn_disagree(self):
        self.repo.write("photon-core/adapters/quic/quic.go", 'const defaultALPN = "photon-quic-v1"\n')
        self.repo.write("photon-core/tests/transportlab/bridgesim/main.go",
                        'NextProtos:   []string{"photon-quic-v2"},\n')
        self.repo.commit("alpn")
        self.assertViolation(self.check(), "ALPN differs")

    def test_protocol_constant(self):
        self.repo.write("photon-core/services/session/codec.go", "const ProtocolVersion = 0\n")
        self.repo.commit("p0")
        self.assertViolation(self.check(), "ProtocolVersion must be a positive")

    def test_gradle_version(self):
        self.repo.write("photon-sdk/build.gradle.kts", 'version = "0.1.0"\n')
        self.repo.commit("gradle")
        self.assertViolation(self.check(), "declares a version")


class TestRemoteAndSumdb(Base):
    def setUp(self):
        super().setUp()
        self.remote = os.path.join(self.tmp, "remote.git")
        subprocess.run(["git", "init", "-q", "--bare", self.remote], check=True)
        self.repo.git("remote", "add", "origin", self.remote)
        self.repo.release("core", "v0.1.0")
        self.repo.git("push", "-q", "origin", "main", "--tags")
        self.published = {}

    def lookup(self, base, mod, ver):
        return self.published.get((mod, ver))

    def publish(self, tag):
        """Record the tag's hashes the way sum.golang.org would."""
        d, _, v = tag.rpartition("/")
        g = cv.Git(self.repo.dir)
        commit = g.rev(tag)
        self.published[(P + d, v)] = cv.module_hashes(g, commit, d, P + d, v, g.go_mod_dirs(commit))

    def test_remote_in_sync(self):
        self.assertClean(self.check(remote="origin"))

    def test_moved_published_tag(self):
        self.repo.commit("later")
        self.repo.git("tag", "-d", "core/v0.1.0")
        self.repo.tag("core/v0.1.0")
        self.assertViolation(self.check(remote="origin"), "was moved or recreated")

    def test_remote_tag_missing_locally(self):
        self.repo.git("tag", "-d", "core/v0.1.0")
        self.assertViolation(self.check(remote="origin"), "not locally")

    def test_sumdb_match_and_reuse(self):
        self.publish("core/v0.1.0")
        self.assertClean(self.check(sumdb="x", sumdb_lookup=self.lookup))
        # Same version, different content: the tag is deleted and recreated
        # on a changed tree (as if the remote ruleset had been bypassed).
        self.repo.write("core/core.go", "package core // changed\n")
        self.repo.commit("changed")
        self.repo.git("tag", "-d", "core/v0.1.0")
        self.repo.tag("core/v0.1.0")
        self.assertViolation(self.check(sumdb="x", sumdb_lookup=self.lookup),
                             "published with different content")

    def test_release_gate(self):
        self.repo.write("core/core.go", "package core // fix\n")
        self.repo.write("core/CHANGELOG.md", changelog("v0.1.0", "v0.1.1"))
        self.repo.commit("fix")
        kw = dict(remote="origin", sumdb="x", sumdb_lookup=self.lookup)
        self.assertClean(self.release("core/v0.1.1", **kw))
        # Old or reused versions. With the tag present locally, `release`
        # treats it as the tag being released, and the remote refuses it.
        self.assertViolation(self.release("core/v0.1.0", **kw), "already exists on origin")
        self.assertViolation(self.release("core/v0.0.9", **kw), "allowed next")
        self.assertViolation(self.release("core/v0.1.0-rc.1", **kw), "already released")
        # Gap, missing notes, bad format.
        self.assertViolation(self.release("core/v0.3.0", **kw), "allowed next")
        self.assertViolation(self.release("core/v0.2.0", **kw), "no dated, non-empty section")
        self.assertViolation(self.release("core/v0.1.1-beta", **kw), "is not vMAJOR")

    def test_release_of_deleted_but_published_version(self):
        # The tag is gone from both sides, but the checksum database keeps the
        # version forever: releasing it again must fail.
        self.publish("core/v0.1.0")
        self.repo.git("push", "-q", "origin", ":refs/tags/core/v0.1.0")
        self.repo.git("tag", "-d", "core/v0.1.0")
        self.assertViolation(self.release("core/v0.1.0", remote="origin", sumdb="x",
                                          sumdb_lookup=self.lookup),
                             "already in the checksum database")

    def test_release_requires_tagged_dependency(self):
        self.repo.write("rt/go.mod", "module %srt\n\nrequire %score v0.2.0\n" % (P, P))
        self.repo.write("rt/CHANGELOG.md", changelog("v0.1.0"))
        self.repo.commit("rt")
        self.assertViolation(self.release("rt/v0.1.0"), "tag the dependency first")

    def test_pushed_verification(self):
        self.repo.release("core", "v0.1.1")
        c = self.release("core/v0.1.1", pushed=True, remote="origin")
        self.assertViolation(c, "is not on origin")
        self.repo.git("push", "-q", "origin", "main", "--tags")
        self.assertClean(self.release("core/v0.1.1", pushed=True, remote="origin"))

    def test_cli_exit_codes(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(cv.main(["--repo", self.repo.dir, "--main-ref", "main", "check"]), 0)
            self.assertEqual(cv.main(["--repo", self.repo.dir, "--main-ref", "main",
                                      "release", "core/v0.1.0", "--remote", "origin"]), 1)
        self.assertIn("violation(s)", out.getvalue())


@unittest.skipUnless(os.path.isdir(os.path.join(REPO_ROOT, ".git")) and
                     subprocess.run(["git", "-C", REPO_ROOT, "rev-parse", "-q", "--verify",
                                     "refs/tags/photon-core/v0.1.0"],
                                    capture_output=True).returncode == 0,
                     "needs the Photon repository with its tags")
class TestRealRepository(unittest.TestCase):
    # The values sum.golang.org holds for the first public release
    # (lookup github.com/wiloris/photon/<module>@v0.1.0, 2026-10-26).
    PUBLISHED = {
        "photon-core": ("h1:HGH+9gLH1Srpl0xgtalWIxD5GofOw5o2x2mCVRVvSi8=",
                        "h1:pMPVdrO//PmUTQKpfCv89O7h4nF7c6L8XQeQzNrZKII="),
        "photon-plugin-sdk": ("h1:Td56LIpzaCw8c3tT0rrlEJ6w5yIaYMc6yWdDwfn5oAM=",
                              "h1:WDpN3r7G+sksI6CbG3Wg1OUx9rKYVEpXAmptyPel4ek="),
        "photon-plugin-runtime": ("h1:7mXN4M/0RVfYhBlqvOyrff5ZhUV28s8cT/k9tWfILfE=",
                                  "h1:oB/7dVFcZMVyvL5sT0n3NGC4txWRP9Lw64FApL0lm90="),
        "photon-bindings/c": ("h1:QBA7eCXF1WlrZsMrsb3a8C2P6irIAjy5RFJXteEvSfI=",
                              "h1:+NvC/uqTi0hEW71xKG9tYcFpyy6Hd82zYb+UGjzFH1I="),
    }

    def test_module_hash_matches_checksum_database(self):
        g = cv.Git(REPO_ROOT)
        for d, want in self.PUBLISHED.items():
            commit = g.rev("%s/v0.1.0" % d)
            got = cv.module_hashes(g, commit, d, P + d, "v0.1.0", g.go_mod_dirs(commit))
            self.assertEqual(got, want, d)


if __name__ == "__main__":
    unittest.main()
