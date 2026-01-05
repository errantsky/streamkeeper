# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-01-04

### Added

- Full HTTP protocol implementation (PUT, POST, GET, DELETE, HEAD)
- JSON mode with array flattening for `application/json` streams
- Long-polling support with `live=true` query parameter
- Server-Sent Events (SSE) support with `live=sse` query parameter
- Stream TTL and expiration via `Stream-TTL` and `Stream-Expires-At` headers
- Sequence ordering enforcement via `Stream-Seq` header
- ETag-based caching with `If-None-Match` support
- OTP supervision tree with per-stream GenServer processes
- ETS-based storage backend
- Phoenix.PubSub integration for live update notifications
- `DurableStreams.StreamManager` programmatic API
- `DurableStreams.Protocol.Plug` for Phoenix integration
- `DurableStreams.Protocol.V1Plug` for standalone HTTP server
- `mix durable_streams.conformance` task for running conformance tests
- 100% conformance with official Durable Streams protocol tests (131/131)
