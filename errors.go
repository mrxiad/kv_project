package kv

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("index update failed")
	ErrKeyNotFound       = errors.New("key not found in database")
	ErrDataFileNotFound  = errors.New("data file is not found")
)
