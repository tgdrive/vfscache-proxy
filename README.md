# VFS Cache Proxy

A high-performance streaming proxy server built on top of [Rclone's VFS](https://rclone.org/commands/rclone_mount/#vfs-file-caching) layer. This service allows you to stream files from any HTTP/HTTPS URL with the benefits of Rclone's smart caching, buffering, and read-ahead capabilities.

It also features an internal metadata cache using `freecache` to minimize upstream requests for file size and modification times.

## Features

- **Rclone VFS Integration**: Leverages Rclone's robust VFS for disk caching, sparse file support, and efficient streaming.
- **Smart Metadata Caching**: In-memory caching of file size and modification times to reduce latency and upstream API calls.
- **Flexible Input**: Supports passing target URLs via query parameters or Base64-encoded paths.
- **Deduplication**: Optional query parameter stripping to cache identical files with different access tokens together.
- **Docker Ready**: minimal Alpine-based Docker image.

## Getting Started

### Prerequisites

- Docker

### Installation & Run

You can run the VFS Proxy directly using Docker. This will pull the image (if hosted) or you can build it locally.

To run the server with a persistent cache directory:

```bash
docker run -d \
  -p 8080:8080 \
  -v /path/to/host/cache:/app/cache \
  vfs-proxy --cache-dir /app/cache
```

You can pass any CLI flags (see below) to the end of the docker run command.

## CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8080` | Port to listen on. |
| `--cache-dir` | System Temp | Directory to store the VFS disk cache. |
| `--chunk-size` | `64M` | The chunk size for read requests. |
| `--chunk-streams` | `2` | Number of parallel streams to read at once. |
| `--max-age` | `1h` | Max age of files in the VFS cache. |
| `--max-size` | `off` | Max total size of objects in the cache. |
| `--strip-query` | `false` | If true, strips query parameters from the URL when generating the cache key. Useful for signed URLs. |
| `--strip-domain` | `false` | If true, strips domain and protocol from the URL when generating the cache key. Useful for content mirrored across multiple domains. |
| `--metadata-cache-size` | `5M` | Size of the in-memory metadata cache. |

## API Endpoints

### 1. Stream via Query Parameter

```http
GET /stream?url=https://example.com/video.mp4
```

### 2. Stream via Base64 Path

Useful for tools or players that struggle with query parameters in URLs.

1. Base64 encode your target URL (URL-safe or standard).
   - `https://example.com/video.mp4` -> `aHR0cHM6Ly9leGFtcGxlLmNvbS92aWRlby5tcDQ`
2. Request the path:

```http
GET /stream/aHR0cHM6Ly9leGFtcGxlLmNvbS92aWRlby5tcDQ
```

## Caddy Plugin

VFS Cache Proxy can be used as a Caddy module. This allows Caddy to act as a high-performance caching reverse proxy for specific upstreams.

### 1. Build with xcaddy

Since this is a Go plugin, you must compile it into Caddy:

```bash
xcaddy build \
    --with github.com/tgdrive/vfscache-proxy/caddy
```

### 2. Caddyfile Usage

#### Pattern A: Simple Reverse Proxy
Acts as a transparent caching layer for an upstream server.

```caddyfile
:8080 {
    vfs https://upstream.com {
        cache_dir /var/cache/vfs
        max_age 24h
    }
}
```
*Request:* `GET /movie.mp4` -> *Fetches & Caches:* `https://upstream.com/movie.mp4`

#### Pattern B: Specific Route
Only proxy specific paths through the VFS layer.

```caddyfile
example.com {
    route /media/* {
        vfs https://s3.amazonaws.com/my-bucket {
            chunk_size 128M
            chunk_streams 4
        }
    }
    
    # Other standard Caddy logic
    file_server browse
}
```

#### Pattern C: Stripping Prefix (`handle_path`)
Useful if the upstream does not expect the local path prefix.

```caddyfile
:8080 {
    handle_path /stream/* {
        vfs https://cdn.example.net {
            strip_domain
            strip_query
        }
    }
}
```

### Caddyfile Directives

| Directive | Description |
|-----------|-------------|
| `upstream` (arg) | **Required**. The base URL of the source server. |
| `cache_dir` | Directory for the VFS disk cache. |
| `max_age` | Max age of files in cache (e.g., `1h`, `24h`). |
| `max_size` | Max total size of the disk cache. |
| `chunk_size` | Chunk size for read requests (default `64M`). |
| `chunk_streams` | Parallel download streams per request. |
| `strip_query` | Strip query parameters for the metadata cache key. |
| `strip_domain` | Strip domain/protocol for the metadata cache key. |
| `metadata_cache_size` | Size of the in-memory metadata cache (default `5M`). |
| `fs_name` | Custom name for the VFS file system. |
| `cache_mode` | VFS cache mode (`off`, `minimal`, `writes`, `full`). |

---

## How it Works

1. **Request**: The server receives a request for a URL.
2. **Metadata**: It checks an in-memory `freecache` for the file's size and modification time.
   - If missing, it performs an HTTP `HEAD` (or `GET` range) request to the upstream URL and caches the result for 1 hour.
3. **VFS Mount**: The file is virtually "mounted" using the `link` backend.
4. **Streaming**: Rclone's VFS layer handles the reading, downloading chunks in parallel (`--chunk-streams`), and caching them to disk (`--cache-dir`).
5. **Response**: The content is streamed to the client with support for Range requests (seeking).
