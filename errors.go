package kv

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("the key is empty")
	ErrIndexUpdateFailed = errors.New("index update failed")
	ErrKeyNotFound       = errors.New("key not found in database")
	ErrDataFileNotFound  = errors.New("data file is not found")
	ErrOptionsInvalid    = errors.New("options is invalid")
	ErrBatchTooLarge     = errors.New("batch is too large")
)
