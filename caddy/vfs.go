package vfs

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"

	"github.com/tgdrive/vfscache-proxy/pkg/vfsproxy"
)

func init() {
	caddy.RegisterModule(VFS{})
	httpcaddyfile.RegisterHandlerDirective("vfs", parseCaddyfile)
}

// VFS implements a Caddy HTTP handler that proxies requests to a VFS backend.
type VFS struct {
	Upstream          string `json:"upstream,omitempty"`
	FsName            string `json:"fs_name,omitempty"`
	CacheDir          string `json:"cache_dir,omitempty"`
	CacheMaxAge       string `json:"cache_max_age,omitempty"`
	CacheMaxSize      string `json:"cache_max_size,omitempty"`
	CacheChunkSize    string `json:"cache_chunk_size,omitempty"`
	CacheChunkStreams int    `json:"cache_chunk_streams,omitempty"`
	StripQuery        bool   `json:"strip_query,omitempty"`
	StripDomain       bool   `json:"strip_domain,omitempty"`
	MetadataCacheSize string `json:"metadata_cache_size,omitempty"`

	// New Options
	CacheMode         string `json:"cache_mode,omitempty"`
	WriteWait         string `json:"write_wait,omitempty"`
	ReadWait          string `json:"read_wait,omitempty"`
	WriteBack         string `json:"write_back,omitempty"`
	DirCacheTime      string `json:"dir_cache_time,omitempty"`
	FastFingerprint   bool   `json:"fast_fingerprint,omitempty"`
	CacheMinFreeSpace string `json:"cache_min_free_space,omitempty"`
	CaseInsensitive   bool   `json:"case_insensitive,omitempty"`
	ReadOnly          bool   `json:"read_only,omitempty"`
	NoModTime         bool   `json:"no_modtime,omitempty"`
	NoChecksum        bool   `json:"no_checksum,omitempty"`
	NoSeek            bool   `json:"no_seek,omitempty"`
	DirPerms          string `json:"dir_perms,omitempty"`
	FilePerms         string `json:"file_perms,omitempty"`

	handler *vfsproxy.Handler
	logger  *zap.Logger
}

// CaddyModule returns the Caddy module information.
func (VFS) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.vfs",
		New: func() caddy.Module { return &VFS{} },
	}
}

// Provision sets up the VFS handler.
func (v *VFS) Provision(ctx caddy.Context) error {
	v.logger = ctx.Logger(v)

	opt := vfsproxy.Options{
		FsName:            v.FsName,
		CacheDir:          v.CacheDir,
		CacheMaxAge:       v.CacheMaxAge,
		CacheMaxSize:      v.CacheMaxSize,
		CacheChunkSize:    v.CacheChunkSize,
		CacheChunkStreams: v.CacheChunkStreams,
		StripQuery:        v.StripQuery,
		StripDomain:       v.StripDomain,
		MetadataCacheSize: v.MetadataCacheSize,
		CacheMode:         v.CacheMode,
		WriteWait:         v.WriteWait,
		ReadWait:          v.ReadWait,
		WriteBack:         v.WriteBack,
		DirCacheTime:      v.DirCacheTime,
		FastFingerprint:   v.FastFingerprint,
		CacheMinFreeSpace: v.CacheMinFreeSpace,
		CaseInsensitive:   v.CaseInsensitive,
		ReadOnly:          v.ReadOnly,
		NoModTime:         v.NoModTime,
		NoChecksum:        v.NoChecksum,
		NoSeek:            v.NoSeek,
		DirPerms:          v.DirPerms,
		FilePerms:         v.FilePerms,
	}

	handler, err := vfsproxy.NewHandler(opt)
	if err != nil {
		return fmt.Errorf("failed to create VFS handler: %v", err)
	}

	v.handler = handler
	return nil
}

// Validate ensures the configuration is valid.
func (v *VFS) Validate() error {
	return nil
}

// Cleanup cleans up the VFS resources.
func (v *VFS) Cleanup() error {
	if v.handler != nil {
		v.handler.Shutdown()
	}
	return nil
}

// ServeHTTP serves the HTTP request.
func (v *VFS) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	fullURL := v.Upstream
	if !strings.HasSuffix(fullURL, "/") && !strings.HasPrefix(r.URL.Path, "/") {
		fullURL += "/"
	}
	fullURL += r.URL.Path

	v.handler.Serve(w, r, fullURL)
	return nil
}

// parseCaddyfile parses the Caddyfile configuration.
// Syntax:
//
//	vfs <upstream> {
//	    cache_dir <path>
//	    max_age <duration>
//	    max_size <size>
//	    chunk_size <size>
//	    chunk_streams <number>
//	    strip_query
//	}
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	var v VFS
	err := v.UnmarshalCaddyfile(h.Dispenser)
	return &v, err
}

// UnmarshalCaddyfile sets up the handler from Caddyfile tokens.
func (v *VFS) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	for d.Next() {
		if d.NextArg() {
			v.Upstream = d.Val()
		}
		if v.Upstream == "" {
			return d.Err("missing upstream URL")
		}

		for d.NextBlock(0) {
			switch d.Val() {
			case "fs_name":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.FsName = d.Val()
			case "cache_dir":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.CacheDir = d.Val()
			case "max_age":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.CacheMaxAge = d.Val()
			case "max_size":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.CacheMaxSize = d.Val()
			case "chunk_size":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.CacheChunkSize = d.Val()
			case "chunk_streams":
				if !d.NextArg() {
					return d.ArgErr()
				}
				streams, err := strconv.Atoi(d.Val())
				if err != nil {
					return d.Errf("invalid chunk_streams: %v", err)
				}
				v.CacheChunkStreams = streams
			case "strip_query":
				v.StripQuery = true
			case "strip_domain":
				v.StripDomain = true
			case "metadata_cache_size":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.MetadataCacheSize = d.Val()
			case "cache_mode":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.CacheMode = d.Val()
			case "write_wait":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.WriteWait = d.Val()
			case "read_wait":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.ReadWait = d.Val()
			case "write_back":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.WriteBack = d.Val()
			case "dir_cache_time":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.DirCacheTime = d.Val()
			case "fast_fingerprint":
				v.FastFingerprint = true
			case "min_free_space":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.CacheMinFreeSpace = d.Val()
			case "case_insensitive":
				v.CaseInsensitive = true
			case "read_only":
				v.ReadOnly = true
			case "no_modtime":
				v.NoModTime = true
			case "no_checksum":
				v.NoChecksum = true
			case "no_seek":
				v.NoSeek = true
			case "dir_perms":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.DirPerms = d.Val()
			case "file_perms":
				if !d.NextArg() {
					return d.ArgErr()
				}
				v.FilePerms = d.Val()
			default:
				return d.Errf("unknown subdirective '%s'", d.Val())
			}
		}
	}
	return nil
}

// Interface guards
var (
	_ caddy.Provisioner           = (*VFS)(nil)
	_ caddy.Validator             = (*VFS)(nil)
	_ caddy.CleanerUpper          = (*VFS)(nil)
	_ caddyhttp.MiddlewareHandler = (*VFS)(nil)
	_ caddyfile.Unmarshaler       = (*VFS)(nil)
)
