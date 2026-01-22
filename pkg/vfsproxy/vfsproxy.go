package vfsproxy

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/tgdrive/vfscache-proxy/backend/link"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

type Options struct {
	FsName            string
	CacheDir          string
	CacheMaxAge       string
	CacheMaxSize      string
	CacheChunkSize    string
	CacheChunkStreams int
	StripQuery        bool
	StripDomain       bool
	MetadataCacheSize string

	// Additional VFS Options
	CacheMode         string
	WriteWait         string
	ReadWait          string
	WriteBack         string
	DirCacheTime      string
	FastFingerprint   bool
	CacheMinFreeSpace string
	CaseInsensitive   bool
	ReadOnly          bool
	NoModTime         bool
	NoChecksum        bool
	NoSeek            bool
	DirPerms          string
	FilePerms         string
}

type Handler struct {
	VFS       *vfs.VFS
	hashCache map[string]string
	mu        sync.RWMutex

	stripQuery  bool
	stripDomain bool
}

func NewHandler(opt Options) (*Handler, error) {

	regInfo, _ := fs.Find("link")
	if regInfo == nil {
		return nil, fmt.Errorf("could not find link backend")
	}

	backendOpt := configmap.Simple{
		"strip_query":  fmt.Sprintf("%v", opt.StripQuery),
		"strip_domain": fmt.Sprintf("%v", opt.StripDomain),
		"cache_size":   fmt.Sprintf("%v", opt.MetadataCacheSize),
	}

	f, err := regInfo.NewFs(context.Background(), opt.FsName, "", backendOpt)
	if err != nil {
		return nil, err
	}

	m := configmap.Simple{
		"vfs_cache_mode":           opt.CacheMode,
		"vfs_cache_max_age":        opt.CacheMaxAge,
		"vfs_cache_max_size":       opt.CacheMaxSize,
		"vfs_read_chunk_size":      opt.CacheChunkSize,
		"dir_cache_time":           opt.DirCacheTime,
		"vfs_read_chunk_streams":   fmt.Sprintf("%d", opt.CacheChunkStreams),
		"vfs_write_wait":           opt.WriteWait,
		"vfs_read_wait":            opt.ReadWait,
		"vfs_write_back":           opt.WriteBack,
		"vfs_fast_fingerprint":     fmt.Sprintf("%v", opt.FastFingerprint),
		"vfs_cache_min_free_space": opt.CacheMinFreeSpace,
		"vfs_case_insensitive":     fmt.Sprintf("%v", opt.CaseInsensitive),
		"read_only":                fmt.Sprintf("%v", opt.ReadOnly),
		"no_modtime":               fmt.Sprintf("%v", opt.NoModTime),
		"no_checksum":              fmt.Sprintf("%v", opt.NoChecksum),
		"no_seek":                  fmt.Sprintf("%v", opt.NoSeek),
		"dir_perms":                opt.DirPerms,
		"file_perms":               opt.FilePerms,
	}

	if m["vfs_cache_mode"] == "" {
		m["vfs_cache_mode"] = "full"
	}
	if m["dir_cache_time"] == "" {
		m["dir_cache_time"] = "0s"
	}

	vfsOpt := vfscommon.Opt
	if err := configstruct.Set(m, &vfsOpt); err != nil {
		return nil, fmt.Errorf("failed to parse VFS options: %w", err)
	}

	actualCacheDir := opt.CacheDir
	if actualCacheDir == "" {
		actualCacheDir = filepath.Join(os.TempDir(), "rclone_vfs_cache")
	}
	_ = config.SetCacheDir(actualCacheDir)

	vfsInstance := vfs.New(f, &vfsOpt)
	return &Handler{
		VFS:         vfsInstance,
		hashCache:   make(map[string]string),
		stripQuery:  opt.StripQuery,
		stripDomain: opt.StripDomain,
	}, nil
}

func (h *Handler) Shutdown() {
	h.VFS.Shutdown()
}

func (h *Handler) Serve(w http.ResponseWriter, r *http.Request, targetURL string) {
	if targetURL == "" {
		http.Error(w, "Target URL is required", http.StatusBadRequest)
		return
	}

	fileHash := h.getFileHash(targetURL)

	// Only register if not already present to avoid redundant map operations
	if _, exists := link.Load(fileHash); !exists {
		link.Register(fileHash, targetURL)
	}

	h.ServeFile(w, r, fileHash)
}

func (h *Handler) getFileHash(targetURL string) string {
	h.mu.RLock()
	fileHash, exists := h.hashCache[targetURL]
	h.mu.RUnlock()

	if exists {
		return fileHash
	}

	// Apply stripping to the URL before hashing
	keyURL := link.StripURL(targetURL, h.stripQuery, h.stripDomain)

	hashBytes := md5.Sum([]byte(keyURL))
	fileHash = fmt.Sprintf("%x", hashBytes)

	h.mu.Lock()
	h.hashCache[targetURL] = fileHash
	h.mu.Unlock()

	return fileHash
}

func (h *Handler) ServeFile(w http.ResponseWriter, r *http.Request, remote string) {
	ctx := r.Context()
	node, err := h.VFS.Stat(remote)
	if err == vfs.ENOENT {
		fs.Infof(remote, "%s: File not found", r.RemoteAddr)
		http.Error(w, "File not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "Failed to find file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if !node.IsFile() {
		http.Error(w, "Not a file", http.StatusNotFound)
		return
	}

	entry := node.DirEntry()
	if entry == nil {
		http.Error(w, "Can't open file being written", http.StatusNotFound)
		return
	}
	obj := entry.(fs.Object)
	file := node.(*vfs.File)

	knownSize := obj.Size() >= 0
	if knownSize {
		w.Header().Set("Content-Length", strconv.FormatInt(node.Size(), 10))
	}

	mimeType := fs.MimeType(ctx, obj)
	if mimeType == "application/octet-stream" && path.Ext(remote) == "" {
	} else {
		w.Header().Set("Content-Type", mimeType)
	}

	w.Header().Set("Last-Modified", file.ModTime().UTC().Format(http.TimeFormat))

	if r.Method == "HEAD" {
		return
	}

	// open the object
	in, err := file.Open(os.O_RDONLY)
	if err != nil {
		http.Error(w, "Failed to open file: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		err := in.Close()
		if err != nil {
			fs.Errorf(remote, "Failed to close file: %v", err)
		}
	}()

	if knownSize {
		http.ServeContent(w, r, remote, node.ModTime(), in)
	} else {
		if rangeRequest := r.Header.Get("Range"); rangeRequest != "" {
			http.Error(w, "Can't use Range: on files of unknown length", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		n, err := io.Copy(w, in)
		if err != nil {
			fs.Errorf(obj, "Didn't finish writing GET request (wrote %d/unknown bytes): %v", n, err)
			return
		}
	}
}
