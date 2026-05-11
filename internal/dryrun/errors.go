package dryrun

import "fmt"

type ErrorKind int

const (
	ErrConnection ErrorKind = iota
	ErrAuth
	ErrPrivilege
	ErrVersionParse
	ErrIntrospection
	ErrHistory
	ErrConfig
	ErrDatabase
	ErrReplicaCapture
)

type Error struct {
	Kind    ErrorKind
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error { return e.Err }

func NewError(kind ErrorKind, msg string) *Error {
	return &Error{Kind: kind, Message: msg}
}

func WrapError(kind ErrorKind, msg string, err error) *Error {
	return &Error{Kind: kind, Message: msg, Err: err}
}
