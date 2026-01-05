# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-01-05

### Added

- **Retention policies** for automatic stream compaction
  - `max_age`: Remove messages older than specified duration (milliseconds)
  - `max_messages`: Keep at most N messages in stream
  - `max_bytes`: Keep at most N bytes of data in stream
- `410 Gone` HTTP response for compacted offsets with `Stream-Earliest-Offset` header
- `DurableStreams.Retention.Supervisor` - OTP supervisor for retention subsystem
- `DurableStreams.Retention.Scheduler` - Periodic GenServer for compaction scheduling
- `DurableStreams.Retention.Worker` - Stateless module for stream compaction
- `Offset.timestamp/1` function to extract timestamp from offsets
- Storage functions for retention queries:
  - `get_first_message_timestamp/1`
  - `find_offset_after_timestamp/2`
  - `find_offset_after_n_messages/2`
  - `find_offset_after_n_bytes/2`
  - `delete_messages_before/2`
  - `update_after_compaction/4`
  - `list_streams_with_retention/0`

### Changed

- Stream struct now tracks `message_count`, `total_bytes`, and `earliest_offset`
- Storage `append/3` now updates message count and byte tracking
- Application supervision tree now includes Retention.Supervisor

## [0.1.0] - 2025-01-05

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
- `DurableStreams.LiveView` helper module for Phoenix LiveView integration
- `mix durable_streams.conformance` task for running conformance tests
- 100% conformance with official Durable Streams protocol tests (131/131)

### Notes

- Package name is `streamkeeper`, module namespace is `DurableStreams`
- Phoenix LiveView is an optional dependency (module only compiles when available)
