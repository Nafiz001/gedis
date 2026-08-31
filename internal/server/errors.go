package server

// respError is an error whose message is already a complete RESP error reply,
// leading machine-readable code included.
//
// Command handlers run their logic inside store.Update callbacks, which can
// only signal failure by returning an error. Carrying the exact wire message in
// the error means the reply is decided where the problem is detected, instead
// of being reconstructed from a generic error at the call site.
type respError string

func (e respError) Error() string { return string(e) }

const (
	errNotInteger    = respError("ERR value is not an integer or out of range")
	errNotFloat      = respError("ERR value is not a valid float")
	errIncrOverflow  = respError("ERR increment or decrement would overflow")
	errNaNOrInf      = respError("ERR increment would produce NaN or Infinity")
	errStringTooLong = respError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")

	// Hash fields get their own wording in Redis rather than reusing the
	// string-value errors, and clients match on the text.
	errHashNotInteger = respError("ERR hash value is not an integer")
	errHashNotFloat   = respError("ERR hash value is not a float")
)
