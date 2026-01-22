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
	"sync"
	"time"

	"github.com/coocood/freecache"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
)

var retryErrorCodes = []int{
	429, // Too Many Requests
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

func shouldRetry(resp *http.Response, err error) (bool, error) {
	if err != nil {
		return fserrors.ShouldRetry(err), err
	}
	if resp != nil {
		for _, code := range retryErrorCodes {
			if resp.StatusCode == code {
				return true, nil
			}
		}
	}
	return false, nil
}

var (
	errorReadOnly = errors.New("link: read only")
	urlMap        sync.Map
)

type entry struct {
	url    string
	header http.Header
}

func Register(remote, url string, header http.Header) {
	urlMap.Store(remote, &entry{url: url, header: header})
}

func Load(remote string) (string, bool) {
	val, ok := urlMap.Load(remote)
	if !ok {
		return "", false
	}
	return val.(*entry).url, true
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
	pacer       *fs.Pacer
}

func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	f := &Fs{
		name:  name,
		root:  root,
		pacer: fs.NewPacer(ctx, pacer.NewDefault()),
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
	} else {
		f.cache = freecache.NewCache(5 * 1024 * 1024)
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
	e := val.(*entry)
	u := e.url

	// Generate cache key from URL hash to avoid duplication
	keyURL := StripURL(u, f.stripQuery, f.stripDomain)

	keyHash := md5.Sum([]byte(keyURL))
	cacheKey := keyHash[:]

	if f.cache != nil {
		if val, err := f.cache.Get(cacheKey); err == nil && len(val) == 16 {
			size := int64(binary.LittleEndian.Uint64(val[:8]))
			modTime := time.Unix(0, int64(binary.LittleEndian.Uint64(val[8:])))
			return &Object{
				fs:      f,
				remote:  remote,
				url:     u,
				header:  e.header,
				size:    size,
				modTime: modTime,
			}, nil
		}
	}

	client := fshttp.NewClient(ctx)

	var resp *http.Response
	var err error

	newReq := func(method, urlStr string) (*http.Request, error) {
		req, err := http.NewRequestWithContext(ctx, method, urlStr, nil)
		if err != nil {
			return nil, err
		}
		if e.header != nil {
			if v := e.header.Get("Authorization"); v != "" {
				req.Header.Set("Authorization", v)
			}
			if v := e.header.Get("Cookie"); v != "" {
				req.Header.Set("Cookie", v)
			}
		}
		return req, nil
	}

	// Try HEAD first
	req, err := newReq("HEAD", u)
	if err != nil {
		return nil, err
	}

	err = f.pacer.Call(func() (bool, error) {
		resp, err = client.Do(req)
		retry, _ := shouldRetry(resp, err)
		return retry, err
	})

	// Fallback to GET if HEAD is not allowed or supported, or if size is unknown (-1)
	if err != nil || (resp == nil || resp.StatusCode != http.StatusOK) || resp.ContentLength < 0 {
		req, err = newReq("GET", u)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Range", "bytes=0-0")
		err = f.pacer.Call(func() (bool, error) {
			resp, err = client.Do(req)
			retry, _ := shouldRetry(resp, err)
			return retry, err
		})
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
	if f.cache != nil {
		f.cache.Set(cacheKey, buf, 3600)
	}

	return &Object{
		fs:      f,
		remote:  remote,
		url:     u,
		header:  e.header,
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
	header   http.Header
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
	if o.header != nil {
		if v := o.header.Get("Authorization"); v != "" {
			req.Header.Set("Authorization", v)
		}
		if v := o.header.Get("Cookie"); v != "" {
			req.Header.Set("Cookie", v)
		}
	}
	for k, v := range fs.OpenOptionHeaders(options) {
		req.Header.Add(k, v)
	}

	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = client.Do(req)
		retry, _ := shouldRetry(resp, err)
		return retry, err
	})

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
