# Security Policy

## Reporting a vulnerability

Please **do not** report security vulnerabilities in public GitHub issues,
pull requests or discussions.

Report them privately through GitHub: open the repository's **Security** tab
and choose **Report a vulnerability** (GitHub private vulnerability
reporting). Only the maintainer can see such a report.

Please include:

- the affected component (for example `photon-core`, `photon-bindings/c`,
  `photon-plugin-runtime`) and the commit you tested;
- a description of the issue and its impact;
- steps to reproduce or a proof of concept, if you have one.

You will receive an acknowledgement as soon as the maintainer has read the
report. Please give the maintainer a reasonable time to investigate and
release a fix before disclosing the issue publicly.

## Supported versions

Photon has no tagged releases yet. Security fixes are made on the `main`
branch.

## Scope notes

Some protections are documented as not implemented; reports about them are
still welcome, but they are known limitations rather than new findings:

- plugin process isolation is implemented on Linux only
  (`photon-plugin-runtime/docs/sandbox.md`);
- there is no standalone Photon server in this repository.
