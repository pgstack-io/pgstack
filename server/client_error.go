package main

const (
	PG_ERROR_SEVERITY_ERROR = "ERROR"
	PG_ERROR_SEVERITY_FATAL = "FATAL"

	PG_ERROR_CODE_INTERNAL_ERROR                      = "XX000"
	PG_ERROR_CODE_INVALID_CATALOG_NAME                = "3D000"
	PG_ERROR_CODE_INVALID_AUTHORIZATION_SPECIFICATION = "28000"
	PG_ERROR_CODE_SYNTAX_ERROR                        = "42601"
	PG_ERROR_CODE_FEATURE_NOT_SUPPORTED               = "0A000"
	PG_ERROR_CODE_PROTOCOL_VIOLATION                  = "08P01"
	PG_ERROR_CODE_INVALID_PARAMETER_VALUE             = "22023"
)

type ClientError struct {
	Severity string
	Code     string
	Message  string
	cause    error
}

func NewClientError(severity, code, message string, cause error) *ClientError {
	return &ClientError{
		Severity: severity,
		Code:     code,
		Message:  message,
		cause:    cause,
	}
}

func (clientError *ClientError) Error() string {
	if clientError.cause != nil {
		return clientError.cause.Error()
	}
	return clientError.Message
}

func (clientError *ClientError) Unwrap() error {
	return clientError.cause
}
