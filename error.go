package fti

import (
	"fmt"
)

type Error struct {
	rawError    error
	parentError *Error
	description string
	isTemporary bool
}

func (e *Error) Error() string {
	if e.rawError == nil {
		errorStr := e.description
		if e.parentError != nil {
			errorStr = errorStr + fmt.Sprintf("\n%s", e.parentError.Error())
		}
		return errorStr
	} else {
		return e.rawError.Error()
	}
}

func (e *Error) Temporary() bool {
	return e.isTemporary
}
