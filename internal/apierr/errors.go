// Package apierr defines the stable error codes used by the HTTP API and by
// the internal components that need to report a machine-readable failure.
package apierr

import (
	"errors"
	"net/http"
)

// Code is a stable machine-readable error code.
type Code string

const (
	CodeInvalidRequest      Code = "INVALID_REQUEST"
	CodeAgentNotFound       Code = "AGENT_NOT_FOUND"
	CodeAgentDisabled       Code = "AGENT_DISABLED"
	CodeQueueFull           Code = "QUEUE_FULL"
	CodeAgentQuotaExceeded  Code = "AGENT_QUOTA_EXCEEDED"
	CodeTaskNotFound        Code = "TASK_NOT_FOUND"
	CodeTaskAlreadyFinished Code = "TASK_ALREADY_FINISHED"
	CodeGatewayShuttingDown Code = "GATEWAY_SHUTTING_DOWN"
	CodeInternalError       Code = "INTERNAL_ERROR"
)

var httpStatus = map[Code]int{
	CodeInvalidRequest:      http.StatusBadRequest,
	CodeAgentNotFound:       http.StatusNotFound,
	CodeAgentDisabled:       http.StatusConflict,
	CodeQueueFull:           http.StatusTooManyRequests,
	CodeAgentQuotaExceeded:  http.StatusTooManyRequests,
	CodeTaskNotFound:        http.StatusNotFound,
	CodeTaskAlreadyFinished: http.StatusConflict,
	CodeGatewayShuttingDown: http.StatusTooManyRequests,
	CodeInternalError:       http.StatusInternalServerError,
}

// StatusFor returns the HTTP status code associated with code.
func StatusFor(code Code) int {
	if s, ok := httpStatus[code]; ok {
		return s
	}
	return http.StatusInternalServerError
}

// Error is an API-level error with a stable code and an HTTP status.
type Error struct {
	Code    Code
	Message string
	Status  int
}

func (e *Error) Error() string {
	return string(e.Code) + ": " + e.Message
}

// New builds an *Error using the canonical HTTP status for code.
func New(code Code, message string) *Error {
	return &Error{Code: code, Message: message, Status: StatusFor(code)}
}

// From converts any error into an *Error. Unknown errors become INTERNAL_ERROR.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return New(CodeInternalError, err.Error())
}
