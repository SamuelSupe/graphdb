package httpapi

import (
	"context"
	"errors"
	"net/http"
)

const statusClientClosedRequest = 499

func (s *Server) writeExecutionContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := s.WriteExecutionTimeout
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}

func writeRequestError(w http.ResponseWriter, err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, context.Canceled):
		writeErrorErr(w, statusClientClosedRequest, err)
	default:
		writeErrorErr(w, http.StatusGatewayTimeout, err)
	}
}
