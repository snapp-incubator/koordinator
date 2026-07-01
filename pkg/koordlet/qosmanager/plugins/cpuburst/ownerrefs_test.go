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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// errReader is a client.Reader whose Get always fails (here with NotFound,
// standing in for a deleted/orphaned owner). List is unused by the resolver.
type errReader struct{}

func (errReader) Get(ctx context.Context, key types.NamespacedName, obj client.Object, opts ...client.GetOption) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "replicasets"}, key.Name)
}

func (errReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return fmt.Errorf("list not supported")
}

// TestOwnerResolver_ResolveTopOwnerName_NoControllerOwner verifies that a pod
// with no controller owner fails resolution, rather than falling back to the
// pod's own name. A zero-value ownerResolver suffices because the no-controller
// path returns before touching the reader or cache.
func TestOwnerResolver_ResolveTopOwnerName_NoControllerOwner(t *testing.T) {
	r := &ownerResolver{}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "standalone-pod", Namespace: "default"},
	}
	name, err := r.ResolveTopOwnerName(pod)
	assert.Empty(t, name, "no owner name should be returned on resolution failure")
	assert.Error(t, err, "resolution should fail for a pod with no controller owner")
	assert.Contains(t, err.Error(), "no controller owner",
		"error should describe the no-controller-owner condition, got: %v", err)
}

// TestOwnerResolver_ResolveTopOwnerName_NilPod verifies the nil-pod guard.
func TestOwnerResolver_ResolveTopOwnerName_NilPod(t *testing.T) {
	r := &ownerResolver{}
	name, err := r.ResolveTopOwnerName(nil)
	assert.Empty(t, name)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pod is nil", "want a nil-pod error, got: %v", err)
}

// TestOwnerResolver_ResolveTopOwnerName_FetchError verifies that when an owner
// in the chain can't be fetched (here: NotFound, standing in for a deleted owner
// / CRD-not-installed / RBAC-denied), resolution fails instead of best-effort
// returning the last reachable owner.
func TestOwnerResolver_ResolveTopOwnerName_FetchError(t *testing.T) {
	r := &ownerResolver{
		reader: errReader{},
		cache:  newOwnerCache(ownerCacheTTL, ownerCacheCap),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "owned-pod",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs-1", Controller: ptr.To(true)},
			},
		},
	}
	name, err := r.ResolveTopOwnerName(pod)
	assert.Empty(t, name, "no owner name should be returned on fetch failure")
	assert.Error(t, err, "resolution should fail when an owner can't be fetched")
	assert.Contains(t, err.Error(), "fetch owner", "want a fetch-owner error, got: %v", err)
}

// TestOwnerResolver_ResolveTopOwnerName_FetchErrorNotCached verifies that a
// failed resolution is not cached, so a transient error is retried on the next
// call rather than pinned as "owner unknown".
func TestOwnerResolver_ResolveTopOwnerName_FetchErrorNotCached(t *testing.T) {
	r := &ownerResolver{
		reader: errReader{},
		cache:  newOwnerCache(ownerCacheTTL, ownerCacheCap),
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "owned-pod",
			Namespace: "default",
			OwnerReferences: []metav1.OwnerReference{
				{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "rs-1", Controller: ptr.To(true)},
			},
		},
	}
	if _, err := r.ResolveTopOwnerName(pod); err == nil {
		t.Fatal("expected first resolution to fail")
	}
	// The cache must remain empty: a second call should still attempt the walk
	// (and fail again) rather than return a stale "owner unknown" entry.
	assert.Equal(t, 0, r.cache.len(), "failed resolutions must not be cached")
	if _, err := r.ResolveTopOwnerName(pod); err == nil {
		t.Fatal("expected second resolution to fail too (failure not retried from cache)")
	}
}

// ensure errReader satisfies client.Reader at compile time.
var _ client.Reader = errReader{}
