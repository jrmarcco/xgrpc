package errs

import "errors"

var ErrInvalidEtcdLeaseTTL = errors.New("[xgrpc] etcd lease TTL must be greater than 0")
