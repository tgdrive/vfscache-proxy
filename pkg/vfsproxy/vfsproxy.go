package vfsproxy

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

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
	MetadataCacheSize int

	// Additional VFS Options
	CacheMode         string
	CachePollInterval string
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
	VFS *vfs.VFS
}

func NewHandler(opt Options) (*Handler, error) {
	// Initialize global link cache
	if opt.MetadataCacheSize > 0 {
		link.InitCache(opt.MetadataCacheSize)
	}

	regInfo, _ := fs.Find("link")
	if regInfo == nil {
		return nil, fmt.Errorf("could not find link backend")
	}

	// Backend options
	backendOpt := configmap.Simple{
		"strip_query":  fmt.Sprintf("%v", opt.StripQuery),
		"strip_domain": fmt.Sprintf("%v", opt.StripDomain),
	}

	fsName := opt.FsName
	if fsName == "" {
		fsName = "vfs-streaming-proxy"
	}

	f, err := regInfo.NewFs(context.Background(), fsName, "", backendOpt)
	if err != nil {
		return nil, err
	}

	// Map flags to Rclone VFS options
	m := configmap.Simple{
		"vfs_cache_mode":           opt.CacheMode,
		"vfs_cache_max_age":        opt.CacheMaxAge,
		"vfs_cache_max_size":       opt.CacheMaxSize,
		"vfs_read_chunk_size":      opt.CacheChunkSize,
		"dir_cache_time":           opt.DirCacheTime,
		"vfs_read_chunk_streams":   fmt.Sprintf("%d", opt.CacheChunkStreams),
		"vfs_cache_poll_interval":  opt.CachePollInterval,
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

	// Set defaults if empty
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

	// Setup Cache Directory
	actualCacheDir := opt.CacheDir
	if actualCacheDir == "" {
		actualCacheDir = filepath.Join(os.TempDir(), "rclone_vfs_cache")
	}
	// Note: This sets the global Rclone config cache dir.
	_ = config.SetCacheDir(actualCacheDir)

	vfsInstance := vfs.New(f, &vfsOpt)
	return &Handler{VFS: vfsInstance}, nil
}

func (h *Handler) Shutdown() {
	h.VFS.Shutdown()
}

// Serve handles the VFS streaming for a specific target URL.
// It is intended to be called by a frontend (main.go or Caddy plugin)
// that has already resolved the target URL.
func (h *Handler) Serve(w http.ResponseWriter, r *http.Request, targetURL string) {
	if targetURL == "" {
		http.Error(w, "Target URL is required", http.StatusBadRequest)
		return
	}

	hashBytes := md5.Sum([]byte(targetURL))
	fileHash := fmt.Sprintf("%x", hashBytes)

	log.Printf("VFS Serve: %s -> %s", targetURL, fileHash)
	link.Register(fileHash, targetURL)

	handle, err := h.VFS.OpenFile(fileHash, os.O_RDONLY, 0)
	if err != nil {
		log.Printf("VFS error: %v", err)
		http.Error(w, "File error", http.StatusNotFound)
		return
	}
	defer handle.Close()

	info, err := handle.Stat()
	if err != nil {
		http.Error(w, "Stat error", http.StatusInternalServerError)
		return
	}

	http.ServeContent(w, r, fileHash, info.ModTime(), handle)
}
