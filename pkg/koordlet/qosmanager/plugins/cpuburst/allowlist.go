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
	"strings"
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

	if _, err := os.Stat(configDir); err != nil {
		klog.Warningf("cpuBurst allowlist config directory %v not accessible: %v", configDir, err)
		return nil
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
		// Best-effort: still load the initial allowlist once even though we
		// can't watch for changes. The pod can be restarted to recover.
		w.loadAllowlist()
		return nil
	}

	// Load the initial allowlist
	w.loadAllowlist()

	go w.syncLoop(stopCh)

	return nil
}

// syncLoop is the main event loop for fsnotify events.
func (w *AllowlistWatcher) syncLoop(stopCh <-chan struct{}) {
	// Debounce timer: coalesce rapid symlink-swap events into a single reload
	// (turns several events per ConfigMap change into one reload).
	var debounceTimer *time.Timer

	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Chmod > 0 {
				continue
			}
			name := filepath.Base(event.Name)
			// React to the ..data / ..data_tmp symlinks swapped by the kubelet's
			// atomic ConfigMap update.
			if strings.HasPrefix(name, "..data") {
				// Debounce rapid successive events
				if debounceTimer != nil {
					debounceTimer.Stop()
				}

				klog.V(5).Infof("cpuBurst allowlist config change detected: %v %v", event.Op, event.Name)
				debounceTimer = time.AfterFunc(100*time.Millisecond, func() {
					w.loadAllowlist()
					klog.V(4).Infof("cpuBurst allowlist reloaded from %v, %d entries", w.configPath, w.entryCount())
				})
			}

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			klog.Warningf("cpuBurst allowlist watcher error: %v", err)

		case <-stopCh:
			if debounceTimer != nil {
				debounceTimer.Stop()
			}
			w.watcher.Close()
			return
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
			klog.Warningf("failed to read cpuBurst allowlist config file %v: %v", w.configPath, err)
		}
		// File doesn't exist yet (optional ConfigMap) - keep current allowlist
		return
	}

	var config podAllowlistConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		klog.Warningf("failed to parse cpuBurst allowlist config file %v: %v; keeping previous allowlist", w.configPath, err)
		// Keep previous allowlist on parse error - don't clear it
		return
	}

	newAllowlist := make(map[string]map[string]struct{})
	for _, entry := range config.PodAllowlist {
		if entry.Namespace == "" || entry.GenerateName == "" {
			klog.V(5).Infof("skipping cpuBurst allowlist entry with empty namespace or generateName: %+v", entry)
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

	klog.V(4).Infof("loaded cpuBurst allowlist from %v: %d entries across %d namespaces",
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
