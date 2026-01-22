package link

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/coocood/freecache"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
)

var (
	errorReadOnly = errors.New("link: read only")
	urlMap        sync.Map
)

func Register(remote, url string) {
	urlMap.Store(remote, url)
}

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "link",
		Description: "Multi-Link Dynamic Backend",
		NewFs:       NewFs,
	})
}

type Fs struct {
	name        string
	root        string
	features    *fs.Features
	stripQuery  bool
	stripDomain bool
	cache       *freecache.Cache
}

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f := &Fs{
		name: name,
		root: root,
	}
	// Parse backend options
	if val, ok := m.Get("strip_query"); ok && val == "true" {
		f.stripQuery = true
	}
	if val, ok := m.Get("strip_domain"); ok && val == "true" {
		f.stripDomain = true
	}
	if val, ok := m.Get("cache_size"); ok && val != "" {
		var s fs.SizeSuffix
		s.Set(val)
		f.cache = freecache.NewCache(int(s))
	}

	f.features = (&fs.Features{
		ReadMetadata: true,
	}).Fill(ctx, f)
	return f, nil
}

func (f *Fs) Name() string             { return f.name }
func (f *Fs) Root() string             { return f.root }
func (f *Fs) String() string           { return "link:" }
func (f *Fs) Precision() time.Duration { return time.Second }
func (f *Fs) Hashes() hash.Set         { return hash.Set(hash.None) }
func (f *Fs) Features() *fs.Features   { return f.features }

func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	if dir != "" {
		return nil, fs.ErrorDirNotFound
	}
	var entries fs.DirEntries
	urlMap.Range(func(key, value any) bool {
		remote := key.(string)
		obj, err := f.NewObject(ctx, remote)
		if err == nil {
			entries = append(entries, obj)
		}
		return true
	})
	return entries, nil
}

func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	val, ok := urlMap.Load(remote)
	if !ok {
		return nil, fs.ErrorObjectNotFound
	}
	u := val.(string)

	// Generate cache key from URL hash to avoid duplication
	keyURL := u
	if f.stripQuery || f.stripDomain {
		if parsedURL, err := url.Parse(u); err == nil {
			if f.stripQuery {
				parsedURL.RawQuery = ""
			}
			if f.stripDomain {
				parsedURL.Scheme = ""
				parsedURL.Host = ""
			}
			keyURL = parsedURL.String()
		}
	}

	keyHash := md5.Sum([]byte(keyURL))
	cacheKey := keyHash[:]

	if val, err := f.cache.Get(cacheKey); err == nil && len(val) == 16 {
		size := int64(binary.LittleEndian.Uint64(val[:8]))
		modTime := time.Unix(0, int64(binary.LittleEndian.Uint64(val[8:])))
		return &Object{
			fs:      f,
			remote:  remote,
			url:     u,
			size:    size,
			modTime: modTime,
		}, nil
	}

	client := fshttp.NewClient(ctx)

	// Create request with a common User-Agent
	newReq := func(method, urlStr string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, method, urlStr, nil)
		if err != nil {
			return nil, err
		}
		return req, nil
	}

	// Try HEAD first
	req, err := newReq("HEAD", u)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)

	// Fallback to GET if HEAD is not allowed or supported, or if size is unknown (-1)
	if err != nil || (resp.StatusCode != http.StatusOK) || resp.ContentLength < 0 {
		req, err = newReq("GET", u)
		if err != nil {
			return nil, err
		}
		// Try a slightly different range format that is more widely accepted
		req.Header.Set("Range", "bytes=0-0")
		resp, err = client.Do(req)
		if err != nil {
			log.Printf("GET range failed for %s: %v", remote, err)
			return nil, err
		}
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		log.Printf("Metadata fetch failed for %s: status %d (URL: %s)", remote, resp.StatusCode, u)
		return nil, fmt.Errorf("metadata fetch failed: status %d", resp.StatusCode)
	}

	size := resp.ContentLength
	if resp.StatusCode == http.StatusPartialContent {
		var contentRange = resp.Header.Get("Content-Range")
		if contentRange != "" {
			var start, end, total int64
			_, err := fmt.Sscanf(contentRange, "bytes %d-%d/%d", &start, &end, &total)
			if err == nil {
				size = total
			}
		}
	}

	modTime := time.Now()
	if lastMod := resp.Header.Get("Last-Modified"); lastMod != "" {
		if t, err := http.ParseTime(lastMod); err == nil {
			modTime = t
		}
	}

	if size < 0 {
		return nil, fmt.Errorf("metadata fetch failed: unknown file size for %s", u)
	}

	// Store in cache
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[:8], uint64(size))
	binary.LittleEndian.PutUint64(buf[8:], uint64(modTime.UnixNano()))
	f.cache.Set(cacheKey, buf, 3600) // 1 hour expirationhour expiration

	return &Object{
		fs:      f,
		remote:  remote,
		url:     u,
		size:    size,
		modTime: modTime,
	}, nil
}

func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error { return nil }
func (f *Fs) Rmdir(ctx context.Context, dir string) error { return errorReadOnly }

type Object struct {
	fs       *Fs
	remote   string
	url      string
	size     int64
	modTime  time.Time
	mimeType string
}

func (o *Object) Fs() fs.Info    { return o.fs }
func (o *Object) String() string { return o.remote }
func (o *Object) Remote() string { return o.remote }
func (o *Object) Hash(ctx context.Context, r hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}
func (o *Object) Size() int64                                             { return o.size }
func (o *Object) ModTime(ctx context.Context) time.Time                   { return o.modTime }
func (o *Object) MimeType(ctx context.Context) string                     { return o.mimeType }
func (o *Object) Storable() bool                                          { return true }
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error { return errorReadOnly }
func (o *Object) Remove(ctx context.Context) error                        { return errorReadOnly }
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errorReadOnly
}

func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	client := fshttp.NewClient(ctx)
	req, err := http.NewRequestWithContext(ctx, "GET", o.url, nil)
	if err != nil {
		return nil, err
	}
	for k, v := range fs.OpenOptionHeaders(options) {
		req.Header.Add(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		resp.Body.Close()
		return nil, fmt.Errorf("GET failed: %s (status %d)", resp.Status, resp.StatusCode)
	}
	return resp.Body, nil
}

var _ fs.Fs = &Fs{}
var _ fs.Object = &Object{}
