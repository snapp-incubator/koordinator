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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiext "github.com/koordinator-sh/koordinator/apis/extension"
)

const (
	// maxOwnerDepth bounds the owner-reference walk so a malformed/cyclic chain
	// can never loop forever.
	maxOwnerDepth = 3
	// ownerFetchTimeout caps each API GET during the walk so a slow apiserver
	// can't stall the reconcile loop.
	ownerFetchTimeout = 5 * time.Second
	// ownerCacheTTL bounds how long a resolved top owner is trusted before the
	// chain is walked again (handles rare controller-adoption changes).
	ownerCacheTTL = 5 * time.Minute
	// ownerCacheCap bounds memory: when the cache grows beyond this many
	// distinct immediate owners (e.g. many rollouts over time) it is flushed.
	ownerCacheCap = 1024
)

// podOwnerResolver maps a pod to the name of its top-level (original) owner,
// e.g. the Deployment/StatefulSet/Argo Rollout that ultimately controls it.
// It is an interface so tests can inject a fake without a real API client.
type podOwnerResolver interface {
	// ResolveTopOwnerName returns the name of the topmost controller in the
	// pod's ownerReference chain, or an error if it can't be reliably
	// determined -- e.g. the pod has no controller owner, or an owner in the
	// chain can't be fetched (NotFound, CRD not installed/discovered, RBAC
	// denied, ...).
	ResolveTopOwnerName(pod *corev1.Pod) (string, error)
}

// ownerResolver walks pod ownerReferences up to the original parent, fetching
// each level as an unstructured object so any workload kind (Deployment,
// StatefulSet, DaemonSet, ReplicaSet, Argo Rollout, Kruise CloneSet, ...) is
// handled uniformly without per-kind typed clients.
type ownerResolver struct {
	reader client.Reader
	cache  *ownerCache
}

func newOwnerResolver(restConf *rest.Config) (*ownerResolver, error) {
	c, err := client.New(restConf, client.Options{})
	if err != nil {
		return nil, fmt.Errorf("create owner resolver client: %w", err)
	}
	return &ownerResolver{
		reader: c,
		cache:  newOwnerCache(ownerCacheTTL, ownerCacheCap),
	}, nil
}

// ResolveTopOwnerName implements podOwnerResolver.
func (r *ownerResolver) ResolveTopOwnerName(pod *corev1.Pod) (string, error) {
	if pod == nil {
		return "", fmt.Errorf("pod is nil")
	}
	controller := metav1.GetControllerOf(pod)
	if controller == nil {
		return "", fmt.Errorf("pod %s/%s has no controller owner reference", pod.Namespace, pod.Name)
	}

	key := controllerKey(pod.Namespace, controller)
	if cached, ok := r.cache.get(key); ok {
		return cached, nil
	}

	name, err := r.walk(pod.Namespace, controller)
	if err != nil {
		return "", err
	}
	r.cache.set(key, name)
	return name, nil
}

// walk traverses the ownerReference chain from owner up to the root which has no controller owner.
// It returns an error if any level is unreachable or the chain exceeds maxOwnerDepth.
func (r *ownerResolver) walk(namespace string, owner *metav1.OwnerReference) (string, error) {
	current := owner
	for i := 0; i < maxOwnerDepth; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), ownerFetchTimeout)
		obj, err := r.fetchObjectByRef(ctx, current, namespace)
		cancel()
		if err != nil {
			klog.V(5).Infof("cpuBurst allowlist: failed to fetch owner %s/%s (%s %s): %v",
				namespace, current.Name, current.APIVersion, current.Kind, err)
			return "", fmt.Errorf("fetch owner %s %s/%s: %w", current.Kind, namespace, current.Name, err)
		}
		next := metav1.GetControllerOf(obj)
		if next == nil {
			return current.Name, nil
		}
		current = next
	}
	klog.Warningf("cpuBurst allowlist: owner reference chain for %s/%s exceeded max depth %d",
		namespace, owner.Name, maxOwnerDepth)
	return "", fmt.Errorf("owner reference chain for %s/%s exceeded max depth %d", namespace, owner.Name, maxOwnerDepth)
}

// fetchObjectByRef retrieves the object referenced by ref as unstructured. A missing
// owner is treated as an unresolvable chain and fails closed.
func (r *ownerResolver) fetchObjectByRef(ctx context.Context, ref *metav1.OwnerReference, namespace string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(ref.APIVersion)
	obj.SetKind(ref.Kind)

	key := types.NamespacedName{Namespace: namespace, Name: ref.Name}
	if err := r.reader.Get(ctx, key, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("owner %s %s/%s not found", ref.Kind, namespace, ref.Name)
		}
		return nil, fmt.Errorf("failed to get owner %s %s/%s: %w", ref.Kind, namespace, ref.Name, err)
	}
	return obj, nil
}

// controllerKey is the cache key for a pod's immediate controller owner. It is
// stable for a pod's lifetime (ownerReferences are immutable) and shared across
// all pods of the same controller, so the walk runs at most once per controller
// per TTL window.
func controllerKey(namespace string, ref *metav1.OwnerReference) string {
	return namespace + "/" + ref.APIVersion + "/" + ref.Kind + "/" + ref.Name
}

// ownerCache is a simple TTL map of immediate-owner key -> resolved top owner
// name. Expiry is checked lazily on read; when the entry count exceeds cap the
// map is flushed to bound memory (a node has few distinct controllers, so the
// cap is only reached after many rollouts/redeployments over time).
type ownerCache struct {
	sync.Mutex
	items map[string]ownerCacheEntry
	ttl   time.Duration
	cap   int
}

type ownerCacheEntry struct {
	name      string
	expiresAt time.Time
}

func newOwnerCache(ttl time.Duration, cap int) *ownerCache {
	return &ownerCache{
		items: map[string]ownerCacheEntry{},
		ttl:   ttl,
		cap:   cap,
	}
}

func (c *ownerCache) get(key string) (string, bool) {
	c.Lock()
	defer c.Unlock()
	e, ok := c.items[key]
	if !ok {
		return "", false
	}
	if time.Now().After(e.expiresAt) {
		delete(c.items, key)
		return "", false
	}
	return e.name, true
}

func (c *ownerCache) set(key, name string) {
	c.Lock()
	defer c.Unlock()
	if len(c.items) >= c.cap {
		c.items = make(map[string]ownerCacheEntry, len(c.items)/2+1)
	}
	c.items[key] = ownerCacheEntry{name: name, expiresAt: time.Now().Add(c.ttl)}
}

// len returns the number of cached entries. It is primarily for tests.
func (c *ownerCache) len() int {
	c.Lock()
	defer c.Unlock()
	return len(c.items)
}

// hasKoordAnnotation reports whether the pod carries any annotation with the
// koordinator.sh/ prefix. The CPU burst allowlist is only enforced for such
// pods: they are the ones opting into koordinator-managed per-pod burst config
// (e.g. koordinator.sh/cpuBurst). Pods without such an annotation are not
// subject to the allowlist and fall through to the normal config resolution.
func hasKoordAnnotation(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for k := range pod.Annotations {
		if strings.HasPrefix(k, apiext.DomainPrefix) {
			return true
		}
	}
	return false
}
