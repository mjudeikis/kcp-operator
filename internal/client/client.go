package client

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ client.Client = &SeedingClient{}

// SeedingClient is a client that communicates with the Kubernetes API
// of the seed cluster and persists all objects to a sink Secret.

type SeedingClient struct {
	Delegate client.Client
	sinkName      string
	sinkNamespace string
	
	// In-memory storage
	objects       map[string][]byte
	mu            sync.RWMutex
	
	// Periodic sync control
	syncInterval  time.Duration
	stopCh        chan struct{}
	syncDone      chan struct{}
}

func NewSeedingClient(delegate client.Client, sinkName, sinkNamespace string) *SeedingClient {
	return NewSeedingClientWithInterval(delegate, sinkName, sinkNamespace, 10*time.Second)
}

func NewSeedingClientWithInterval(delegate client.Client, sinkName, sinkNamespace string, syncInterval time.Duration) *SeedingClient {
	c := &SeedingClient{
		Delegate:      delegate,
		sinkName:      sinkName,
		sinkNamespace: sinkNamespace,
		objects:       make(map[string][]byte),
		syncInterval:  syncInterval,
		stopCh:        make(chan struct{}),
		syncDone:      make(chan struct{}),
	}
	
	// Start periodic sync goroutine
	go c.periodicSync()
	
	return c
}

func (c *SeedingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	err := c.Delegate.Create(ctx, obj, opts...)
	if err != nil {
		return err
	}
	
	// Store in memory after successful creation
	if storeErr := c.storeObjectInMemory(obj); storeErr != nil {
		fmt.Printf("Warning: failed to store object in memory: %v\n", storeErr)
	}
	
	return nil
}
func (c *SeedingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	err := c.Delegate.Delete(ctx, obj, opts...)
	if err != nil {
		return err
	}
	
	// Remove from memory after successful deletion
	if removeErr := c.removeObjectFromMemory(obj); removeErr != nil {
		fmt.Printf("Warning: failed to remove object from memory: %v\n", removeErr)
	}
	
	return nil
}

func (c *SeedingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return c.Delegate.DeleteAllOf(ctx, obj, opts...)
}

func (c *SeedingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Delegate.Get(ctx, key, obj, opts...)
	if err != nil {
		return err
	}
	
	// Store in memory after successful get
	if storeErr := c.storeObjectInMemory(obj); storeErr != nil {
		fmt.Printf("Warning: failed to store retrieved object in memory: %v\n", storeErr)
	}
	
	return nil
}

func (c *SeedingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	err := c.Delegate.Patch(ctx, obj, patch, opts...)
	if err != nil {
		return err
	}
	
	// Store in memory after successful patch
	if storeErr := c.storeObjectInMemory(obj); storeErr != nil {
		fmt.Printf("Warning: failed to store patched object in memory: %v\n", storeErr)
	}
	
	return nil
}

func (c *SeedingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	err := c.Delegate.List(ctx, list, opts...)
	if err != nil {
		return err
	}
	
	// Store all listed objects in memory
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	
	for _, item := range items {
		if obj, ok := item.(client.Object); ok {
			if storeErr := c.storeObjectInMemory(obj); storeErr != nil {
				fmt.Printf("Warning: failed to store listed object in memory: %v\n", storeErr)
			}
		}
	}
	
	return nil
}

func (c *SeedingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	err := c.Delegate.Update(ctx, obj, opts...)
	if err != nil {
		return err
	}
	
	// Store in memory after successful update
	if storeErr := c.storeObjectInMemory(obj); storeErr != nil {
		fmt.Printf("Warning: failed to store updated object in memory: %v\n", storeErr)
	}
	
	return nil
}

func (c *SeedingClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return c.Delegate.GroupVersionKindFor(obj)
}

func (c *SeedingClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return c.Delegate.IsObjectNamespaced(obj)
}

func (c *SeedingClient) RESTMapper() meta.RESTMapper {
	return c.Delegate.RESTMapper()
}

func (c *SeedingClient) Scheme() *runtime.Scheme {
	return c.Delegate.Scheme()
}

func (c *SeedingClient) Status() client.StatusWriter {
	return c.Delegate.Status()
}

func (c *SeedingClient) SubResource(subResource string) client.SubResourceClient {
	return c.Delegate.SubResource(subResource)
}

// generateGVRKey generates a Kubernetes API-style path key for storing objects
// Uses underscores instead of forward slashes to comply with Secret key validation
func (c *SeedingClient) generateGVRKey(obj client.Object) (string, error) {
	gvk, err := c.GroupVersionKindFor(obj)
	if err != nil {
		return "", err
	}

	restMapper := c.RESTMapper()
	mapping, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version)
	if err != nil {
		return "", err
	}

	var pathBuilder strings.Builder

	// Build the base path
	if gvk.Group == "" {
		// Core API group
		pathBuilder.WriteString("api_")
		pathBuilder.WriteString(gvk.Version)
	} else {
		// Named API group
		pathBuilder.WriteString("apis_")
		// Replace dots with underscores in group name
		pathBuilder.WriteString(strings.ReplaceAll(gvk.Group, ".", "_"))
		pathBuilder.WriteString("_")
		pathBuilder.WriteString(gvk.Version)
	}

	// Add namespace if object is namespaced
	if mapping.Scope.Name() == meta.RESTScopeNameNamespace {
		namespace := obj.GetNamespace()
		if namespace != "" {
			pathBuilder.WriteString("_namespaces_")
			// Replace hyphens with underscores in namespace name
			pathBuilder.WriteString(strings.ReplaceAll(namespace, "-", "_"))
		}
	}

	// Add resource name
	pathBuilder.WriteString("_")
	pathBuilder.WriteString(mapping.Resource.Resource)

	// Add object name if available
	name := obj.GetName()
	if name != "" {
		pathBuilder.WriteString("_")
		// Replace hyphens with underscores in object name
		pathBuilder.WriteString(strings.ReplaceAll(name, "-", "_"))
	}

	return pathBuilder.String(), nil
}

// Close stops the periodic sync and performs a final sync
func (c *SeedingClient) Close(ctx context.Context) error {
	close(c.stopCh)
	
	// Wait for sync goroutine to finish
	select {
	case <-c.syncDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	
	// Perform final sync
	return c.syncToSecret(ctx)
}

// periodicSync runs in a goroutine and periodically syncs in-memory data to the Secret
func (c *SeedingClient) periodicSync() {
	defer close(c.syncDone)
	
	ticker := time.NewTicker(c.syncInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			if err := c.syncToSecret(ctx); err != nil {
				fmt.Printf("Warning: failed to sync objects to sink Secret: %v\n", err)
			}
			cancel()
		case <-c.stopCh:
			return
		}
	}
}

// syncToSecret synchronizes the in-memory objects to the sink Secret
func (c *SeedingClient) syncToSecret(ctx context.Context) error {
	c.mu.RLock()
	if len(c.objects) == 0 {
		c.mu.RUnlock()
		return nil // Nothing to sync
	}
	
	// Create a copy of the data to avoid holding the lock too long
	objectsCopy := make(map[string][]byte, len(c.objects))
	for k, v := range c.objects {
		objectsCopy[k] = v
	}
	c.mu.RUnlock()
	
	// Get or create the sink Secret
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Name:      c.sinkName,
		Namespace: c.sinkNamespace,
	}
	
	err := c.Delegate.Get(ctx, secretKey, secret)
	if err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Create the secret if it doesn't exist
			secret = &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      c.sinkName,
					Namespace: c.sinkNamespace,
				},
				Data: objectsCopy,
			}
			return c.Delegate.Create(ctx, secret)
		}
		return fmt.Errorf("failed to get sink Secret: %w", err)
	}
	
	// Update the Secret with merged data
	if secret.Data == nil {
		secret.Data = make(map[string][]byte)
	}
	
	// Merge the objects into the secret
	for k, v := range objectsCopy {
		secret.Data[k] = v
	}
	
	return c.Delegate.Update(ctx, secret)
}

// storeObjectInMemory stores the object in memory for later syncing
func (c *SeedingClient) storeObjectInMemory(obj client.Object) error {
	key, err := c.generateGVRKey(obj)
	if err != nil {
		return fmt.Errorf("failed to generate GVR key: %w", err)
	}

	// Serialize the object
	objData, err := json.Marshal(obj)
	if err != nil {
		return fmt.Errorf("failed to marshal object: %w", err)
	}

	c.mu.Lock()
	c.objects[key] = objData
	c.mu.Unlock()

	return nil
}

// removeObjectFromMemory removes the object from in-memory storage
func (c *SeedingClient) removeObjectFromMemory(obj client.Object) error {
	key, err := c.generateGVRKey(obj)
	if err != nil {
		return fmt.Errorf("failed to generate GVR key: %w", err)
	}

	c.mu.Lock()
	delete(c.objects, key)
	c.mu.Unlock()

	return nil
}
