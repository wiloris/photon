# Photon brand guide

This guide defines a consistent, restrained use of the Photon name and logo across the repository, documentation, presentations, and developer-facing materials.

## Brand architecture

- **Photon** is the platform and core technology.
- **Photon Core** refers specifically to the Go core module.
- **ENGRAM** is a client application built on Photon; it is not a synonym for Photon and should not be presented as part of the core.
- **Photon SDK**, **Photon Developer Tools**, and **Photon Plugin Runtime** are component names. Use the full name on first mention when context may be unclear.

Keep this separation visible in diagrams, feature lists, package descriptions, and marketing copy. Do not imply that a capability belongs to the core if it is provided only by an application, bridge, or plugin.

## Logo asset

Primary repository asset:

`assets/branding/photon-logo.png`

Use the supplied artwork as-is. Do not redraw, stretch, skew, rotate, recolor, add effects, crop essential parts, or overlay text on the logo. Preserve its aspect ratio. When resizing, scale width and height together.

### Placement and size

- Prefer a centered placement on title pages, project presentations, and the README.
- Keep clear space around the mark; as a practical minimum, leave whitespace equal to roughly the height of the emblem's narrowest prominent stroke on every side.
- For web and documentation headers, use a size that remains legible without overwhelming the page. The README uses a 360-pixel display width as a presentation choice, not a minimum.
- Do not place the mark on visually noisy backgrounds or on a background that makes its edges difficult to distinguish.
- For dark or light backgrounds, use the supplied asset only when it remains clearly legible. Do not manufacture alternate color variants without approving and storing them as official assets.

## Color and typography

The logo artwork is the visual source of truth for the mark. Until a formal palette and type system are adopted, use a neutral documentation palette:

- page/background: white or near-white;
- primary text: near-black;
- secondary text: neutral gray;
- accent: use sparingly and only when sampled from the approved artwork or a separately approved brand asset.

Do not invent official hex values or claim that a particular font is the official Photon typeface. For documentation, choose a readable system sans-serif for headings and body text, with a monospace face for code, paths, and API names. Maintain clear heading hierarchy, accessible contrast, and restrained use of bold text.

## Naming and writing

- Write the project name as **Photon**, with an initial capital and the remaining letters lowercase.
- Use **Photon Core** for the core when the distinction matters; avoid using “Photon” and “ENGRAM” interchangeably.
- Keep technical claims precise. Distinguish implemented, partially integrated, planned, and experimental functionality.
- Prefer direct, concise descriptions over superlatives such as “revolutionary,” “unbreakable,” or “production-ready” unless a claim is explicitly substantiated.
- Preserve canonical capitalization for API names, module paths, protocol names, and component names.

## Repository and community materials

- Use the primary logo in README pages and high-level project documentation where it adds recognition.
- Link to `BRAND.md` when contributors need logo or naming guidance.
- Do not use the logo to imply endorsement, certification, or affiliation that does not exist.
- Keep screenshots, architecture diagrams, and ENGRAM-specific visuals clearly labelled so readers can tell which product or component they depict.

## Accessibility

- Include descriptive alternative text when the logo is embedded in Markdown or HTML: `alt="Photon logo"`.
- Keep text contrast readable and do not rely on color alone to communicate status.
- Avoid using the logo as the only label for a link or action; provide adjacent descriptive text when needed.

## Changes to this guide

A new logo variant, official color palette, typography system, or usage exception should be added only when the asset or decision has been reviewed and committed to the repository. Update this guide and the relevant README together so contributors have one consistent reference.
