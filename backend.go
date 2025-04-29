package cachestore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type BackendType string

const (
	BackendTypeAny   BackendType = "any"
	BackendTypeBytes BackendType = "bytes"
)

type BackendTypeConstraint interface {
	any | []byte
}

type Backend interface {
	Name() string
	Options() StoreOptions
	Type() BackendType
}

type BackendTyped[T BackendTypeConstraint] interface {
	Backend
	Store[T]
}

func OpenStore[T BackendTypeConstraint](backend Backend, opts ...StoreOptions) Store[T] {
	options := backend.Options()
	if len(opts) > 0 {
		options = ApplyOptions(opts...)
	}

	switch backend := backend.(type) {
	case Store[any]:
		return newBackendAdapter[T](backend, options)
	case Store[[]byte]:
		return newSerializingBackendAdapter[T](backend, options)
	default:
		// NOTE: we return a nil store here, so that the caller can
		// check if the store is nil and return an error. Alternatively,
		// we could update OpenStore() to return a concrete error, but
		// this is more flexible.
		return &backendAdapter[T]{anyStore: nil, options: options}
	}
}

var ErrBackendAdapterNil = fmt.Errorf("cachestore: backend adapter is nil")
var ErrBackendTypeCast = fmt.Errorf("cachestore: backend type cast failure")

func newBackendAdapter[T any](anyStore Store[any], options StoreOptions) Store[T] {
	adapter := &backendAdapter[T]{
		anyStore: anyStore,
		options:  options,
	}
	return adapter
}

type backendAdapter[T any] struct {
	anyStore Store[any]
	options  StoreOptions
}

func (s *backendAdapter[T]) Name() string {
	if s.anyStore == nil {
		return ""
	}
	return s.anyStore.Name()
}

func (s *backendAdapter[T]) Options() StoreOptions {
	return s.options
}

func (s *backendAdapter[T]) Exists(ctx context.Context, key string) (bool, error) {
	if s.anyStore == nil {
		return false, ErrBackendAdapterNil
	}
	return s.anyStore.Exists(ctx, key)
}

func (s *backendAdapter[T]) Set(ctx context.Context, key string, value T) error {
	return s.SetEx(ctx, key, value, s.options.DefaultKeyExpiry)
}

func (s *backendAdapter[T]) SetEx(ctx context.Context, key string, value T, ttl time.Duration) error {
	if s.anyStore == nil {
		return ErrBackendAdapterNil
	}

	return s.anyStore.SetEx(ctx, key, value, ttl)
}

func (s *backendAdapter[T]) BatchSet(ctx context.Context, keys []string, values []T) error {
	return s.BatchSetEx(ctx, keys, values, s.options.DefaultKeyExpiry)
}

func (s *backendAdapter[T]) BatchSetEx(ctx context.Context, keys []string, values []T, ttl time.Duration) error {
	if s.anyStore == nil {
		return ErrBackendAdapterNil
	}

	vs := make([]any, len(values))
	for i := 0; i < len(vs); i++ {
		vs[i] = values[i]
	}
	return s.anyStore.BatchSetEx(ctx, keys, vs, s.options.DefaultKeyExpiry)
}

func (s *backendAdapter[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var v T

	if s.anyStore == nil {
		return v, false, ErrBackendAdapterNil
	}

	// Otherwise, we just use the underlying store's Get method,
	// and type cast to the generic type. This is used by cachestore-mem.
	bv, ok, err := s.anyStore.Get(ctx, key)
	if err != nil {
		return v, ok, err
	}
	if !ok {
		return v, false, nil
	}
	v, ok = bv.(T)
	if !ok {
		// should not happen, but just in case
		return v, false, fmt.Errorf("cachestore: failed to cast value to type %T: %w", v, ErrBackendTypeCast)
	}
	return v, ok, nil
}

func (s *backendAdapter[T]) BatchGet(ctx context.Context, keys []string) ([]T, []bool, error) {
	vs := make([]T, len(keys))
	exists := make([]bool, len(keys))

	if s.anyStore == nil {
		return vs, exists, ErrBackendAdapterNil
	}

	bvs, exists, err := s.anyStore.BatchGet(ctx, keys)
	if err != nil {
		return vs, exists, err
	}

	var ok bool
	for i, v := range bvs {
		if !exists[i] {
			continue
		}
		vs[i], ok = v.(T)
		if !ok {
			// should not happen, but just in case
			return vs, exists, fmt.Errorf("cachestore: failed to cast value to type %T: %w", v, ErrBackendTypeCast)
		}
	}

	return vs, exists, nil
}

func (s *backendAdapter[T]) Delete(ctx context.Context, key string) error {
	if s.anyStore == nil {
		return ErrBackendAdapterNil
	}
	return s.anyStore.Delete(ctx, key)
}

func (s *backendAdapter[T]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if s.anyStore == nil {
		return ErrBackendAdapterNil
	}
	return s.anyStore.DeletePrefix(ctx, keyPrefix)
}

func (s *backendAdapter[T]) ClearAll(ctx context.Context) error {
	if s.anyStore == nil {
		return ErrBackendAdapterNil
	}
	return s.anyStore.ClearAll(ctx)
}

func (s *backendAdapter[T]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (T, error)) (T, error) {
	return s.GetOrSetWithLockEx(ctx, key, getter, s.options.DefaultKeyExpiry)
}

func (s *backendAdapter[T]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (T, error), ttl time.Duration) (T, error) {
	var v T
	if s.anyStore == nil {
		return v, ErrBackendAdapterNil
	}

	g := func(ctx context.Context, key string) (any, error) {
		v, err := getter(ctx, key)
		if err != nil {
			return nil, err
		}
		return v, nil
	}

	bv, err := s.anyStore.GetOrSetWithLockEx(ctx, key, g, ttl)
	if err != nil {
		return v, err
	}
	v, ok := bv.(T)
	if !ok {
		return v, fmt.Errorf("cachestore: failed to cast value to type %T: %w", v, ErrBackendTypeCast)
	}
	return v, nil

}

type serializingBackendAdapter[T any] struct {
	bytesStore Store[[]byte]
	options    StoreOptions
}

func newSerializingBackendAdapter[T any](bytesStore Store[[]byte], options StoreOptions) Store[T] {
	return &serializingBackendAdapter[T]{
		bytesStore: bytesStore,
		options:    options,
	}
}

func (s *serializingBackendAdapter[T]) Name() string {
	if s.bytesStore == nil {
		return ""
	}
	return s.bytesStore.Name()
}

func (s *serializingBackendAdapter[T]) Options() StoreOptions {
	return s.options
}

func (s *serializingBackendAdapter[T]) Exists(ctx context.Context, key string) (bool, error) {
	if s.bytesStore == nil {
		return false, ErrBackendAdapterNil
	}
	return s.bytesStore.Exists(ctx, key)
}

func (s *serializingBackendAdapter[T]) Set(ctx context.Context, key string, value T) error {
	return s.SetEx(ctx, key, value, s.options.DefaultKeyExpiry)
}

func (s *serializingBackendAdapter[T]) SetEx(ctx context.Context, key string, value T, ttl time.Duration) error {
	if s.bytesStore == nil {
		return ErrBackendAdapterNil
	}

	serialized, err := Serialize(value)
	if err != nil {
		return err
	}
	return s.bytesStore.SetEx(ctx, key, serialized, ttl)
}

func (s *serializingBackendAdapter[T]) BatchSet(ctx context.Context, keys []string, values []T) error {
	return s.BatchSetEx(ctx, keys, values, s.options.DefaultKeyExpiry)
}

func (s *serializingBackendAdapter[T]) BatchSetEx(ctx context.Context, keys []string, values []T, ttl time.Duration) error {
	if s.bytesStore == nil {
		return ErrBackendAdapterNil
	}

	serializedValues := make([][]byte, len(values))
	for i, value := range values {
		serialized, err := Serialize(value)
		if err != nil {
			return err
		}
		serializedValues[i] = serialized
	}
	return s.bytesStore.BatchSetEx(ctx, keys, serializedValues, ttl)
}

func (s *serializingBackendAdapter[T]) Get(ctx context.Context, key string) (T, bool, error) {
	var v T
	if s.bytesStore == nil {
		return v, false, ErrBackendAdapterNil
	}

	serialized, ok, err := s.bytesStore.Get(ctx, key)
	if err != nil {
		return v, false, err
	}
	if !ok {
		return v, false, nil
	}

	deserialized, err := Deserialize[T](serialized)
	if err != nil {
		return v, false, err
	}
	return deserialized, true, nil
}

func (s *serializingBackendAdapter[T]) BatchGet(ctx context.Context, keys []string) ([]T, []bool, error) {
	if s.bytesStore == nil {
		return nil, nil, ErrBackendAdapterNil
	}

	serializedValues, exists, err := s.bytesStore.BatchGet(ctx, keys)
	if err != nil {
		return nil, nil, err
	}

	values := make([]T, len(serializedValues))
	for i, serialized := range serializedValues {
		deserialized, err := Deserialize[T](serialized)
		if err != nil {
			return nil, nil, err
		}
		values[i] = deserialized
	}
	return values, exists, nil
}

func (s *serializingBackendAdapter[T]) Delete(ctx context.Context, key string) error {
	if s.bytesStore == nil {
		return ErrBackendAdapterNil
	}
	return s.bytesStore.Delete(ctx, key)
}

func (s *serializingBackendAdapter[T]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if s.bytesStore == nil {
		return ErrBackendAdapterNil
	}
	return s.bytesStore.DeletePrefix(ctx, keyPrefix)
}

func (s *serializingBackendAdapter[T]) ClearAll(ctx context.Context) error {
	if s.bytesStore == nil {
		return ErrBackendAdapterNil
	}
	return s.bytesStore.ClearAll(ctx)
}

func (s *serializingBackendAdapter[T]) GetOrSetWithLock(ctx context.Context, key string, getter func(context.Context, string) (T, error)) (T, error) {
	if s.bytesStore == nil {
		return *new(T), ErrBackendAdapterNil
	}
	return s.GetOrSetWithLockEx(ctx, key, getter, s.options.DefaultKeyExpiry)
}

func (s *serializingBackendAdapter[T]) GetOrSetWithLockEx(ctx context.Context, key string, getter func(context.Context, string) (T, error), ttl time.Duration) (T, error) {
	var v T
	if s.bytesStore == nil {
		return v, ErrBackendAdapterNil
	}

	bytesGetter := func(ctx context.Context, key string) ([]byte, error) {
		v, err := getter(ctx, key)
		if err != nil {
			return nil, err
		}
		return Serialize(v)
	}

	serialized, err := s.bytesStore.GetOrSetWithLockEx(ctx, key, bytesGetter, ttl)
	if err != nil {
		return v, err
	}

	deserialized, err := Deserialize[T](serialized)
	if err != nil {
		return v, err
	}
	return deserialized, nil
}

func Serialize[V any](value V) ([]byte, error) {
	switch v := any(value).(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		out, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("cachestore: failed to marshal data: %w", err)
		}
		return out, nil
	}
}

func Deserialize[V any](data []byte) (V, error) {
	var out V
	switch any(out).(type) {
	case string:
		str := string(data)
		out = any(str).(V)
		return out, nil
	case []byte:
		out = any(data).(V)
		return out, nil
	default:
		err := json.Unmarshal(data, &out)
		if err != nil {
			return out, fmt.Errorf("cachestore: failed to unmarshal data: %w", err)
		}
		return out, nil
	}
}
