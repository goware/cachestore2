package noopcache

import (
	"context"
	"time"

	cachestore "github.com/goware/cachestore2"
)

var _ cachestore.Store[any] = &NoopCache[any]{}

type NoopCache[V any] struct{}

func NewBackend() cachestore.BackendAny {
	return New[any]()
}

func New[V any]() cachestore.Store[V] {
	return &NoopCache[V]{}
}

func (s *NoopCache[V]) Name() string {
	return "noopcache"
}

func (s *NoopCache[V]) Options() cachestore.StoreOptions {
	return cachestore.StoreOptions{}
}

func (s *NoopCache[V]) Exists(ctx context.Context, key string) (bool, error) {
	return false, nil
}

func (s *NoopCache[V]) Set(ctx context.Context, key string, value V) error {
	return nil
}

func (s *NoopCache[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	return nil
}

func (s *NoopCache[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return nil
}

func (s *NoopCache[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	return nil
}

func (s *NoopCache[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V
	return out, false, nil
}

func (s *NoopCache[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	return nil, nil, nil
}

func (s *NoopCache[V]) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *NoopCache[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	return nil
}

func (s *NoopCache[V]) ClearAll(ctx context.Context) error {
	return nil
}

func (s *NoopCache[V]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (V, error)) (V, error) {
	return getter(ctx, key)
}

func (s *NoopCache[V]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration) (V, error) {
	return getter(ctx, key)
}
