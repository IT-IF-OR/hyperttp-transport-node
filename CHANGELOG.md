# Changelog

## [0.1.2] - 2026-11-24

### Added

- Added the `http2` client-option declaration when the Node transport entry point is imported.
- `http2: true` selects `Http2Transport` for both REST and gRPC without modifying `globalThis.fetch`.

## [0.1.1] - 2026-11-23

### Added

- The HTTP/2 transport detects REST and gRPC requests on the same server listener.
- Added HTTP/2 trailer support for gRPC responses.
