package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"plugin"
	"strings"
	"sync"
	"time"

	mr "github.com/LakshyaMittal3301/mapreduce/mapreduce"
)

func main() {

	coordAddr := flag.String("coord-addr", "localhost:8123", "address of the coordinator")
	app := flag.String("app", "", "optional default app name or plugin path (used if coordinator does not send AppName)")
	backend := flag.String("storage", "local", "storage backend: local|s3")
	s3BucketFlag := flag.String("s3-bucket", "", "S3 bucket name")
	s3InputPrefix := flag.String("s3-input-prefix", "inputs/pg", "S3 prefix for input files (ignored; prefix comes from coordinator)")
	s3Concurrency := flag.Int("s3-concurrency", 16, "max concurrent S3 operations")
	logLevel := flag.String("log-level", "info", "log level: info|debug")
	idleWait := flag.Duration("idle-wait", 100*time.Millisecond, "worker idle poll interval")

	flag.Parse()

	cfg := mr.TuningConfig()
	cfg.WorkerIdleWait = *idleWait
	cfg.S3MaxConcurrency = *s3Concurrency
	mr.SetTuning(cfg)

	var storage mr.Storage

	switch *backend {
	case "local":
		storage = mr.NewLocalStorage()
	case "s3":
		_ = s3InputPrefix
		// Input prefix is provided per-task by the coordinator.
		s3Store, err := mr.NewS3Storage(*s3BucketFlag, "")
		if err != nil {
			log.Fatalf("failed to init S3 storage: %v", err)
		}
		storage = s3Store
	default:
		log.Fatalf("unknown storage backend %s", *backend)
	}

	mr.SetLogLevel(*logLevel)

	pluginDir := defaultPluginDir()
	var (
		cacheMu sync.Mutex
		cache   = map[string]mr.AppFuncs{}
	)

	loader := func(appName string) (mr.AppFuncs, error) {
		path, cacheKey, err := resolvePluginPath(appName, *app, pluginDir)
		if err != nil {
			return mr.AppFuncs{}, err
		}

		cacheMu.Lock()
		if funcs, ok := cache[cacheKey]; ok {
			cacheMu.Unlock()
			return funcs, nil
		}
		cacheMu.Unlock()

		funcs, err := loadPlugin(path)
		if err != nil {
			return mr.AppFuncs{}, err
		}

		cacheMu.Lock()
		cache[cacheKey] = funcs
		cacheMu.Unlock()
		return funcs, nil
	}

	mr.Worker(*coordAddr, storage, loader)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (mr.AppFuncs, error) {
	p, err := plugin.Open(filename)
	if err != nil {
		return mr.AppFuncs{}, fmt.Errorf("cannot load plugin %s: %w", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		return mr.AppFuncs{}, fmt.Errorf("cannot find Map in %s: %w", filename, err)
	}
	mapf, ok := xmapf.(func(string, string) []mr.KeyValue)
	if !ok {
		return mr.AppFuncs{}, fmt.Errorf("plugin %s Map has unexpected type", filename)
	}
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		return mr.AppFuncs{}, fmt.Errorf("cannot find Reduce in %s: %w", filename, err)
	}
	reducef, ok := xreducef.(func(string, []string) string)
	if !ok {
		return mr.AppFuncs{}, fmt.Errorf("plugin %s Reduce has unexpected type", filename)
	}

	return mr.AppFuncs{Mapf: mapf, Reducef: reducef}, nil
}

func resolvePluginPath(appName string, defaultApp string, pluginDir string) (string, string, error) {
	if appName == "" {
		if defaultApp == "" {
			return "", "", fmt.Errorf("missing app name (coordinator did not send AppName and no -app provided)")
		}
		if strings.HasSuffix(defaultApp, ".so") || strings.Contains(defaultApp, "/") {
			return defaultApp, defaultApp, nil
		}
		return filepath.Join(pluginDir, defaultApp+".so"), defaultApp, nil
	}
	return filepath.Join(pluginDir, appName+".so"), appName, nil
}

func defaultPluginDir() string {
	exePath, err := os.Executable()
	if err == nil {
		exeDir := filepath.Dir(exePath)
		// If running from repo bin/ directory, plugins live at ../bin/plugins.
		// If running from elsewhere, this still resolves relative to the binary.
		return filepath.Clean(filepath.Join(exeDir, "..", "bin", "plugins"))
	}
	// Fallback to CWD-based path.
	return filepath.Join("bin", "plugins")
}
