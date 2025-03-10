package main

import "errors"

// Define custom error types for better error identification
var (
	ErrReadMessageType   = errors.New("error reading message type")
	ErrReadMessageLength = errors.New("error reading message length")
	ErrReadMessageBody   = errors.New("error reading message body")
	ErrConnectionClosed  = errors.New("connection closed by client") // Or a more specific error type if needed
)

