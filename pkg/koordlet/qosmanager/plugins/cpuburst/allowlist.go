/*
Copyright 2022 The Koordinator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cpuburst

import (
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v2"
	"k8s.io/klog/v2"
)

// PodAllowlistEntry represents one entry in the allowlist ConfigMap.
type PodAllowlistEntry struct {
	Namespace    string `yaml:"namespace"`
	GenerateName string `yaml:"generateName"`
}

// podAllowlistConfig represents the parsed ConfigMap data.
type podAllowlistConfig struct {
	PodAllowlist []PodAllowlistEntry `yaml:"podAllowlist"`
}

// AllowlistWatcher watches a mounted ConfigMap file for the CPU burst allowlist.
// It uses fsnotify to detect changes and provides a thread-safe lookup method.
// The internal storage is a map of namespace -> set of generateNames for O(1) lookup.
type AllowlistWatcher struct {
	sync.RWMutex
	configPath string
	// allowlist stores namespace -> set of generateNames for O(1) lookup.
	allowlist map[string]map[string]struct{}
	watcher   *fsnotify.Watcher
}

func NewAllowlistWatcher(configPath string) *AllowlistWatcher {
	return &AllowlistWatcher{
		configPath: configPath,
		allowlist:  make(map[string]map[string]struct{}),
	}
}

// Run starts the fsnotify watcher on the directory containing the config file.
func (w *AllowlistWatcher) Run(stopCh <-chan struct{}) error {
	configDir := filepath.Dir(w.configPath)

	// If the directory doesn't exist, create it and wait for the ConfigMap to be mounted.
	if _, err := os.Stat(configDir); os.IsNotExist(err) {
		klog.Infof("cpu burst allowlist config directory %v does not exist, creating it", configDir)
		if err := os.MkdirAll(configDir, 0755); err != nil {
			// In case of failure, start a periodic check for the directory to appear
			klog.Warningf("failed to create cpu burst allowlist config directory %v: %v", configDir, err)
			go w.waitForDirectory(stopCh)
			return nil
		}
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	w.watcher = watcher

	if err := w.watcher.Add(configDir); err != nil {
		klog.Warningf("failed to add watch on directory %v: %v", configDir, err)
		w.watcher.Close()
		w.watcher = nil
		go w.waitForDirectory(stopCh)
		return nil
	}

	// Load the initial allowlist
	w.loadAllowlist()

	go w.syncLoop(stopCh)

	return nil
}

// waitForDirectory periodically checks for the config directory to appear,
// then starts the normal watcher. This handles the case where the optional
// ConfigMap hasn't been created yet.
func (w *AllowlistWatcher) waitForDirectory(stopCh <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			configDir := filepath.Dir(w.configPath)
			if _, err := os.Stat(configDir); err == nil {
				klog.Infof("cpu burst allowlist config directory %v now exists, starting watcher", configDir)
				if runErr := w.Run(stopCh); runErr != nil {
					klog.Warningf("failed to start cpu burst allowlist watcher: %v", runErr)
				}
				return
			}
		}
	}
}

// syncLoop is the main event loop for fsnotify events.
func (w *AllowlistWatcher) syncLoop(stopCh <-chan struct{}) {
	configFileName := filepath.Base(w.configPath)

	// Debounce timer: coalesce rapid symlink-swap events into a single reload
	var debounceTimer *time.Timer
	debounceCh := make(chan struct{}, 1)

	triggerReload := func() {
		select {
		case debounceCh <- struct{}{}:
		default:
			// already pending
		}
	}

	for {
		select {
		case <-stopCh:
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			w.watcher.Close()
			return
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			// Ignore Chmod events
			if event.Op&fsnotify.Chmod > 0 {
				continue
			}

			eventName := filepath.Base(event.Name)

			// Trigger reload on events involving:
			// - the target config file (e.g., cpu-burst-allowlist.yaml)
			// - the ..data symlink (K8s ConfigMap atomic update pattern)
			// - ..data_tmp (temporary symlink during atomic update)
			if eventName == configFileName || eventName == "..data" || eventName == "..data_tmp" {
				klog.V(5).Infof("cpu burst allowlist config change detected: %v %v", event.Op, event.Name)
				triggerReload()
			}

			// If a new directory is created inside the watch dir (e.g., ..2026_06_22_...),
			// watch it so we catch writes to files inside it.
			if event.Op&fsnotify.Create > 0 {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					if err := w.watcher.Add(event.Name); err != nil {
						klog.V(5).Infof("failed to add watch on %v: %v", event.Name, err)
					}
				}
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			klog.Warningf("cpu burst allowlist watcher error: %v", err)
		case <-debounceCh:
			// Reset the debounce timer
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			debounceTimer = time.AfterFunc(100*time.Millisecond, func() {
				w.loadAllowlist()
				klog.V(4).Infof("cpu burst allowlist reloaded from %v, %d entries", w.configPath, w.entryCount())
			})
		}
	}
}

// loadAllowlist reads the YAML config file and updates the in-memory allowlist.
// On invalid YAML, it keeps the previous allowlist state (does not clear it).
// On missing file, it keeps the previous allowlist state.
func (w *AllowlistWatcher) loadAllowlist() {
	data, err := os.ReadFile(w.configPath)
	if err != nil {
		if !os.IsNotExist(err) {
			klog.Warningf("failed to read cpu burst allowlist config file %v: %v", w.configPath, err)
		}
		// File doesn't exist yet (optional ConfigMap) - keep current allowlist
		return
	}

	var config podAllowlistConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		klog.Warningf("failed to parse cpu burst allowlist config file %v: %v; keeping previous allowlist", w.configPath, err)
		// Keep previous allowlist on parse error - don't clear it
		return
	}

	newAllowlist := make(map[string]map[string]struct{})
	for _, entry := range config.PodAllowlist {
		if entry.Namespace == "" || entry.GenerateName == "" {
			klog.V(5).Infof("skipping cpu burst allowlist entry with empty namespace or generateName: %+v", entry)
			continue
		}
		if newAllowlist[entry.Namespace] == nil {
			newAllowlist[entry.Namespace] = make(map[string]struct{})
		}
		newAllowlist[entry.Namespace][entry.GenerateName] = struct{}{}
	}

	w.Lock()
	w.allowlist = newAllowlist
	w.Unlock()

	klog.V(4).Infof("loaded cpu burst allowlist from %v: %d entries across %d namespaces",
		w.configPath, w.entryCount(), len(newAllowlist))
}

// IsPodAllowed checks if a pod with the given namespace and generateName is in the allowlist.
func (w *AllowlistWatcher) IsPodAllowed(namespace, generateName string) bool {
	w.RLock()
	defer w.RUnlock()

	nsEntries, ok := w.allowlist[namespace]
	if !ok {
		return false
	}
	_, found := nsEntries[generateName]
	return found
}

// entryCount returns the total number of entries in the allowlist.
func (w *AllowlistWatcher) entryCount() int {
	w.RLock()
	defer w.RUnlock()

	count := 0
	for _, entries := range w.allowlist {
		count += len(entries)
	}
	return count
}
