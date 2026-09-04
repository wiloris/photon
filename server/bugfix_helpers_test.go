package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
)

func newRecorder() *httptest.ResponseRecorder { return httptest.NewRecorder() }

func newActionRequest(body string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/api/v1/action", strings.NewReader(body))
}
