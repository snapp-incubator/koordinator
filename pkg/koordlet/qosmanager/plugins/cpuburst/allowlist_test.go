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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIsPodAllowed(t *testing.T) {
	w := NewAllowlistWatcher("/tmp/nonexistent-for-test")

	// Manually set the allowlist for testing
	w.Lock()
	w.allowlist = map[string]map[string]struct{}{
		"production": {
			"web-app":    {},
			"api-server": {},
		},
		"staging": {
			"test-runner": {},
		},
	}
	w.Unlock()

	tests := []struct {
		name      string
		namespace string
		ownerName string
		expected  bool
	}{
		{
			name:      "matching entry",
			namespace: "production",
			ownerName: "web-app",
			expected:  true,
		},
		{
			name:      "matching entry in different namespace",
			namespace: "staging",
			ownerName: "test-runner",
			expected:  true,
		},
		{
			name:      "namespace matches but ownerName doesn't",
			namespace: "production",
			ownerName: "unknown-app",
			expected:  false,
		},
		{
			name:      "ownerName matches but namespace doesn't",
			namespace: "default",
			ownerName: "web-app",
			expected:  false,
		},
		{
			name:      "neither matches",
			namespace: "default",
			ownerName: "unknown",
			expected:  false,
		},
		{
			name:      "empty namespace",
			namespace: "",
			ownerName: "web-app",
			expected:  false,
		},
		{
			name:      "empty ownerName",
			namespace: "production",
			ownerName: "",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := w.IsPodAllowed(tt.namespace, tt.ownerName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsPodAllowed_EmptyAllowlist(t *testing.T) {
	w := NewAllowlistWatcher("/tmp/nonexistent-for-test")
	// allowlist is empty by default - no pods should be allowed
	assert.False(t, w.IsPodAllowed("production", "web-app"))
	assert.False(t, w.IsPodAllowed("default", "anything"))
}

func TestLoadAllowlist_FileReload(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "cpu-burst-allowlist.yaml")

	// Write initial allowlist
	initialContent := `podAllowlist:
  - namespace: "production"
    ownerName: "web-app"
`
	err := os.WriteFile(configPath, []byte(initialContent), 0644)
	assert.NoError(t, err)

	w := NewAllowlistWatcher(configPath)
	w.loadAllowlist()

	// Verify initial entries
	assert.True(t, w.IsPodAllowed("production", "web-app"))
	assert.False(t, w.IsPodAllowed("staging", "api-server"))

	// Overwrite with new entries
	updatedContent := `podAllowlist:
  - namespace: "staging"
    ownerName: "api-server"
  - namespace: "production"
    ownerName: "worker"
`
	err = os.WriteFile(configPath, []byte(updatedContent), 0644)
	assert.NoError(t, err)

	w.loadAllowlist()

	// Old entry should be gone
	assert.False(t, w.IsPodAllowed("production", "web-app"))
	// New entries should be present
	assert.True(t, w.IsPodAllowed("staging", "api-server"))
	assert.True(t, w.IsPodAllowed("production", "worker"))
}

func TestLoadAllowlist_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "cpu-burst-allowlist.yaml")

	// Write valid initial allowlist
	initialContent := `podAllowlist:
  - namespace: "production"
    ownerName: "web-app"
`
	err := os.WriteFile(configPath, []byte(initialContent), 0644)
	assert.NoError(t, err)

	w := NewAllowlistWatcher(configPath)
	w.loadAllowlist()

	// Verify initial entries
	assert.True(t, w.IsPodAllowed("production", "web-app"))

	// Write invalid YAML
	invalidContent := `podAllowlist:
  - namespace: "production"
    ownerName: "web-app"
  invalid_yaml: [[[
`
	err = os.WriteFile(configPath, []byte(invalidContent), 0644)
	assert.NoError(t, err)

	w.loadAllowlist()

	// Previous allowlist should be preserved on invalid YAML
	assert.True(t, w.IsPodAllowed("production", "web-app"),
		"previous allowlist should be preserved when YAML is invalid")
}

func TestLoadAllowlist_MissingFile(t *testing.T) {
	w := NewAllowlistWatcher("/tmp/nonexistent-path-12345/cpu-burst-allowlist.yaml")

	// Loading from a missing file should not crash and should keep an empty allowlist
	w.loadAllowlist()
	assert.False(t, w.IsPodAllowed("production", "web-app"))
}

func TestLoadAllowlist_EmptyAllowlist(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "cpu-burst-allowlist.yaml")

	// Write empty allowlist
	emptyContent := `podAllowlist: []
`
	err := os.WriteFile(configPath, []byte(emptyContent), 0644)
	assert.NoError(t, err)

	w := NewAllowlistWatcher(configPath)
	w.loadAllowlist()

	assert.False(t, w.IsPodAllowed("production", "web-app"))
	assert.Equal(t, 0, w.entryCount())
}

func TestLoadAllowlist_SkipsEmptyEntries(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "cpu-burst-allowlist.yaml")

	// Write allowlist with entries that have empty namespace or ownerName
	content := `podAllowlist:
  - namespace: "production"
    ownerName: "web-app"
  - namespace: ""
    ownerName: "empty-ns"
  - namespace: "empty-sa"
    ownerName: ""
  - namespace: "staging"
    ownerName: "api"
`
	err := os.WriteFile(configPath, []byte(content), 0644)
	assert.NoError(t, err)

	w := NewAllowlistWatcher(configPath)
	w.loadAllowlist()

	// Valid entries should be present
	assert.True(t, w.IsPodAllowed("production", "web-app"))
	assert.True(t, w.IsPodAllowed("staging", "api"))
	// Entries with empty namespace or ownerName should be skipped
	assert.Equal(t, 2, w.entryCount())
}

func TestAllowlistWatcher_RunWithFileReload(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "cpu-burst-allowlist.yaml")

	// Write the initial allow list
	writeConfigMapVersion(t, tmpDir, 1, `podAllowlist:
  - namespace: "production"
    ownerName: "web-app"
`)

	w := NewAllowlistWatcher(configPath)

	stopCh := make(chan struct{})
	defer close(stopCh)

	assert.NoError(t, w.Run(stopCh))

	// Give the watcher a moment to load the initial file
	time.Sleep(200 * time.Millisecond)

	assert.True(t, w.IsPodAllowed("production", "web-app"))

	// Simulate a kubelet atomic ConfigMap update: new timestamped dir + ..data swap.
	updateConfigMapVersion(t, tmpDir, 2, `podAllowlist:
  - namespace: "staging"
    ownerName: "api-server"
`)

	// Wait for the debounce timer + reload
	time.Sleep(500 * time.Millisecond)

	// Old entry should be gone, new entry should be present
	assert.False(t, w.IsPodAllowed("production", "web-app"))
	assert.True(t, w.IsPodAllowed("staging", "api-server"))
}

// writeConfigMapVersion sets up the initial ConfigMap symlink layout in dir,
// mirroring how the kubelet mounts a ConfigMap volume:
//
//	<dir>/cpu-burst-allowlist.yaml -> ..data/cpu-burst-allowlist.yaml   (stable per-key symlink)
//	<dir>/..data                  -> ..vN/                             (the atomic switch)
//	<dir>/..vN/cpu-burst-allowlist.yaml                              (the real file)
func writeConfigMapVersion(t *testing.T, dir string, version int, content string) {
	t.Helper()
	revDir := filepath.Join(dir, fmt.Sprintf("..v%d", version))
	assert.NoError(t, os.MkdirAll(revDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(revDir, "cpu-burst-allowlist.yaml"), []byte(content), 0644))
	// Per-key symlink -> ..data/cpu-burst-allowlist.yaml (stable across updates).
	assert.NoError(t, os.Symlink(filepath.Join("..data", "cpu-burst-allowlist.yaml"), filepath.Join(dir, "cpu-burst-allowlist.yaml")))
	// ..data -> ..vN (the symlink the watcher reacts to).
	assert.NoError(t, os.Symlink(fmt.Sprintf("..v%d", version), filepath.Join(dir, "..data")))
}

// updateConfigMapVersion simulates a kubelet atomic ConfigMap update: it writes
// the new content into a fresh timestamped directory, then atomically swaps the
// ..data symlink to point at it (via ..data_tmp), exactly as the kubelet does.
func updateConfigMapVersion(t *testing.T, dir string, version int, content string) {
	t.Helper()
	revDir := filepath.Join(dir, fmt.Sprintf("..v%d", version))
	assert.NoError(t, os.MkdirAll(revDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(revDir, "cpu-burst-allowlist.yaml"), []byte(content), 0644))

	// Atomic swap: create ..data_tmp -> new dir, rename it over ..data.
	dataTmp := filepath.Join(dir, "..data_tmp")
	_ = os.Remove(dataTmp) // remove a stale leftover, if any
	assert.NoError(t, os.Symlink(fmt.Sprintf("..v%d", version), dataTmp))
	assert.NoError(t, os.Rename(dataTmp, filepath.Join(dir, "..data")))
}

func TestNewAllowlistWatcher(t *testing.T) {
	w := NewAllowlistWatcher("/some/path/config.yaml")
	assert.NotNil(t, w)
	assert.Equal(t, "/some/path/config.yaml", w.configPath)
	assert.NotNil(t, w.allowlist)
	assert.Equal(t, 0, w.entryCount())
}
