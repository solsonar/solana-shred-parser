# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — Unreleased

Initial release.

### Added

- `ShredListener` — UDP socket with configurable receive buffer.
- `ShredParser` — streaming reassembler that emits `VersionedTransaction`s as soon as enough contiguous shreds arrive.
- Examples: `basic-listener`, `filter-by-program`, `stats`.
