package client

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ client.Client = &SeedingClient{}

// SeedingClient is a client that communicates with the Kubernetes API
// of the seed cluster.

type SeedingClient struct {
	Delegate client.Client
}

func NewSeedingClient(delegate client.Client) *SeedingClient {
	return &SeedingClient{
		Delegate: delegate,
	}
}

func (c *SeedingClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	return c.Delegate.Create(ctx, obj, opts...)
}
func (c *SeedingClient) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	return c.Delegate.Delete(ctx, obj, opts...)
}

func (c *SeedingClient) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	return c.Delegate.DeleteAllOf(ctx, obj, opts...)
}

func (c *SeedingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return c.Delegate.Get(ctx, key, obj, opts...)
}

func (c *SeedingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return c.Delegate.Patch(ctx, obj, patch, opts...)
}

func (c *SeedingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return c.Delegate.List(ctx, list, opts...)
}

func (c *SeedingClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	return c.Delegate.Update(ctx, obj, opts...)
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
