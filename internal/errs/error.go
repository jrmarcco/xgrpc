package errs

import "errors"

var (
	ErrInvalidServiceName   = errors.New("[xgrpc] service name must not be empty and must not contain '/'")
	ErrInvalidEtcdLeaseTTL  = errors.New("[xgrpc] etcd lease TTL must be greater than 0")
	ErrInvalidEtcdKeyPrefix = errors.New("[xgrpc] etcd key prefix must not be empty")
)
