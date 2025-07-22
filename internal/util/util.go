package util

import (
	"strings"
	"time"
)

func StringPtr(s string) *string {
	return &s
}

// create a dotted_order for a run
func NewDottedOrder(id string) string {
	s := time.Now().UTC().Format(time.RFC3339Nano)
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, ":", "")
	s = strings.ReplaceAll(s, ".", "")
	return s + id
}
