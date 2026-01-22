package link

import (
	"net/url"
)

// StripURL removes parts of the URL based on strip options.
// Returns the stripped URL suitable for generating cache keys.
func StripURL(u string, stripQuery, stripDomain bool) string {
	if !stripQuery && !stripDomain {
		return u
	}

	parsedURL, err := url.Parse(u)
	if err != nil {
		return u
	}

	if stripQuery {
		parsedURL.RawQuery = ""
	}
	if stripDomain {
		parsedURL.Scheme = ""
		parsedURL.Host = ""
		parsedURL.User = nil
		parsedURL.Fragment = ""
	}

	return parsedURL.String()
}
