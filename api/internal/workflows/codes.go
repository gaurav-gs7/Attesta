package workflows

import "errors"

// Error codes recorded on a failed run (WorkflowRun.ErrorCode), so callers
// branch on what went wrong rather than on the wording of the message.
const (
	// CodeConflict: the request conflicts with existing state.
	CodeConflict = "conflict"
	// CodeGateClosed: a policy gate refused the operation.
	CodeGateClosed = "gate-closed"
	// CodePersistence: the run could not be recorded.
	CodePersistence = "persistence"
	// CodeInternal: any other failure.
	CodeInternal = "internal"
)

// codedError attaches a code to an error.
type codedError struct {
	code string
	err  error
}

func (e codedError) Error() string { return e.err.Error() }
func (e codedError) Unwrap() error { return e.err }

// WithCode marks err with an error code. A step that fails with it records
// the code on its run.
func WithCode(err error, code string) error {
	if err == nil {
		return nil
	}
	return codedError{code: code, err: err}
}

// CodeOf returns the code carried by err, or CodeInternal.
func CodeOf(err error) string {
	var coded codedError
	if errors.As(err, &coded) {
		return coded.code
	}
	return CodeInternal
}
