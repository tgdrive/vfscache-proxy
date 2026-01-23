package vfs

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"runtime/debug"
	"strconv"

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

	// Register directive order so vfs runs before reverse_proxy
	httpcaddyfile.RegisterDirectiveOrder("vfs", httpcaddyfile.Before, "reverse_proxy")
}

// VFS implements a Caddy HTTP handler that proxies requests to a VFS backend.
type VFS struct {
	// Upstream is the base URL to proxy requests to (required).
	Upstream string `json:"upstream,omitempty"`

	// Passthrough controls whether to call the next handler on 404.
	// If true, when a file is not found, the next handler in the chain is called.
	// If false (default), a 404 response is returned immediately.
	Passthrough bool `json:"passthrough,omitempty"`

	vfsproxy.Options

	handler     *vfsproxy.Handler
	logger      *zap.Logger
	upstreamURL *url.URL
}

// CaddyModule returns the Caddy module information.
func (VFS) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.vfs",
		New: func() caddy.Module {
			return &VFS{
				Options: vfsproxy.DefaultOptions(),
			}
		},
	}
}

// Provision sets up the VFS handler.
func (v *VFS) Provision(ctx caddy.Context) error {
	v.logger = ctx.Logger(v)

	// Parse upstream URL once during provisioning
	parsedURL, err := url.Parse(v.Upstream)
	if err != nil {
		return fmt.Errorf("invalid upstream URL: %w", err)
	}
	v.upstreamURL = parsedURL

	handler, err := vfsproxy.NewHandler(v.Options)
	if err != nil {
		return fmt.Errorf("failed to create VFS handler: %w", err)
	}

	v.handler = handler
	v.logger.Info("VFS handler provisioned",
		zap.String("upstream", v.Upstream),
		zap.String("cache_mode", v.CacheMode),
		zap.String("cache_dir", v.CacheDir),
	)
	return nil
}

// Validate ensures the configuration is valid.
func (v *VFS) Validate() error {
	if v.Upstream == "" {
		return fmt.Errorf("upstream URL is required")
	}

	// Validate upstream URL format
	if v.upstreamURL == nil {
		return fmt.Errorf("upstream URL was not parsed")
	}
	if v.upstreamURL.Scheme != "http" && v.upstreamURL.Scheme != "https" {
		return fmt.Errorf("upstream URL must use http or https scheme, got %q", v.upstreamURL.Scheme)
	}

	// Validate cache_mode if provided
	if v.CacheMode != "" {
		validModes := map[string]bool{"off": true, "minimal": true, "writes": true, "full": true}
		if !validModes[v.CacheMode] {
			return fmt.Errorf("invalid cache_mode %q: must be one of off, minimal, writes, full", v.CacheMode)
		}
	}

	// Validate chunk_streams if provided
	if v.CacheChunkStreams < 0 {
		return fmt.Errorf("chunk_streams must be non-negative, got %d", v.CacheChunkStreams)
	}

	return nil
}

// Cleanup cleans up the VFS resources.
func (v *VFS) Cleanup() error {
	if v.handler != nil {
		v.logger.Info("Shutting down VFS handler")
		v.handler.Shutdown()
	}
	return nil
}

// ServeHTTP serves the HTTP request.
func (v *VFS) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	// Build full URL using url.JoinPath for proper path handling
	fullURL := v.upstreamURL.JoinPath(r.URL.Path).String()
	if r.URL.RawQuery != "" {
		fullURL += "?" + r.URL.RawQuery
	}

	// Wrap in panic recovery
	defer func() {
		if rec := recover(); rec != nil {
			v.logger.Error("panic in ServeHTTP",
				zap.Any("panic", rec),
				zap.String("url", r.URL.String()),
				zap.String("method", r.Method),
				zap.String("stack", string(debug.Stack())),
			)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		}
	}()

	// If passthrough is enabled, use Caddy's ResponseRecorder to buffer 404 responses
	if v.Passthrough && next != nil {
		buf := new(bytes.Buffer)
		shouldBuffer := func(status int, header http.Header) bool {
			return status == http.StatusNotFound
		}
		rec := caddyhttp.NewResponseRecorder(w, buf, shouldBuffer)
		v.handler.Serve(rec, r, fullURL)
		if rec.Buffered() {
			return next.ServeHTTP(w, r)
		}
		return nil
	}

	v.handler.Serve(w, r, fullURL)
	return nil
}

// parseCaddyfile parses the Caddyfile configuration.
func parseCaddyfile(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	v := &VFS{
		Options: vfsproxy.DefaultOptions(),
	}
	err := v.UnmarshalCaddyfile(h.Dispenser)
	return v, err
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
			directive := d.Val()

			switch directive {
			case "passthrough":
				v.Passthrough = true
				continue
			}

			// Try to match directive with Options tags
			found := false
			val := reflect.ValueOf(&v.Options).Elem()
			typ := val.Type()
			for i := 0; i < typ.NumField(); i++ {
				field := typ.Field(i)
				if field.Tag.Get("caddy") == directive {
					f := val.Field(i)
					switch f.Kind() {
					case reflect.Bool:
						f.SetBool(true)
					case reflect.String:
						if !d.NextArg() {
							return d.ArgErr()
						}
						f.SetString(d.Val())
					case reflect.Int:
						if !d.NextArg() {
							return d.ArgErr()
						}
						i, err := strconv.Atoi(d.Val())
						if err != nil {
							return d.Errf("invalid value for %s: %v", directive, err)
						}
						f.SetInt(int64(i))
					}
					found = true
					break
				}
			}

			if !found {
				return d.Errf("unknown subdirective '%s'", directive)
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
