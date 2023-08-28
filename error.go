package minidb

import "errors"

var (
	ErrKeyNotExist   = errors.New("key not found in database")
	ErrInvalidDBFile = errors.New("invalid db file")
)
