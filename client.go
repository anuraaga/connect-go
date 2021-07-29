package rerpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/akshayjshah/rerpc/internal/statuspb/v0"
)

// Doer is the transport-level interface reRPC expects HTTP clients to
// implement. The standard library's http.Client implements Doer.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

type callCfg struct {
	EnableGzipRequest bool
	MaxResponseBytes  int
	Interceptor       CallInterceptor
}

// A CallOption configures a reRPC client or a single call.
//
// In addition to any options grouped in the documentation below, remember that
// Chains and Options are also a valid CallOptions.
type CallOption interface {
	applyToCall(*callCfg)
}

// A Client calls a single method defined by a protocol buffer service. It's
// the interface between the reRPC library and the client code generated by the
// reRPC protoc plugin; most users won't ever need to deal with it directly.
//
// To see an example of how Client is used in the generated code, see the
// internal/pingpb/v0 package.
type Client struct {
	doer   Doer
	url    string
	method string
	opts   []CallOption
}

// NewClient creates a Client. The supplied URL must be the full,
// method-specific URL, without trailing slashes. The supplied method must be a
// fully-qualified protobuf identifier.
//
// For example, the URL https://api.acme.com/acme.foo.v1.Foo/Bar corresponds to
// method acme.foo.v1.Foo.Bar. Remember that NewClient is usually called
// from generated code - most users won't need to deal with long URLs or
// protobuf identifiers directly.
func NewClient(doer Doer, url, method string, opts ...CallOption) *Client {
	return &Client{
		doer:   doer,
		url:    url,
		method: method,
		opts:   opts,
	}
}

// Call the remote procedure.
func (c *Client) Call(ctx context.Context, req, res proto.Message, opts ...CallOption) error {
	var cfg callCfg
	for _, opt := range c.opts {
		opt.applyToCall(&cfg)
	}
	for _, opt := range opts {
		opt.applyToCall(&cfg)
	}

	next := UnaryCall(func(ctx context.Context, req, res proto.Message) error {
		// Take care not to return a typed nil from this function.
		if err := c.call(ctx, req, res, &cfg); err != nil {
			return err
		}
		return nil
	})
	if cfg.Interceptor != nil {
		next = cfg.Interceptor.WrapCall(next)
	}
	spec := &Specification{
		Method:             c.method,
		RequestCompression: CompressionGzip,
	}
	if !cfg.EnableGzipRequest {
		spec.RequestCompression = CompressionIdentity
	}
	reqHeader := make(http.Header, 5)
	reqHeader.Set("User-Agent", UserAgent())
	reqHeader.Set("Content-Type", TypeDefaultGRPC)
	reqHeader.Set("Grpc-Encoding", spec.RequestCompression)
	reqHeader.Set("Grpc-Accept-Encoding", acceptEncodingValue) // always advertise identity & gzip
	reqHeader.Set("Te", "trailers")
	ctx = NewCallContext(ctx, *spec, reqHeader, make(http.Header))
	return next(ctx, req, res)
}

func (c *Client) call(ctx context.Context, req, res proto.Message, cfg *callCfg) *Error {
	md, hasMD := CallMeta(ctx)
	if !hasMD {
		return errorf(CodeInternal, "no call metadata available on context")
	}

	if deadline, ok := ctx.Deadline(); ok {
		untilDeadline := time.Until(deadline)
		if untilDeadline <= 0 {
			return errorf(CodeDeadlineExceeded, "no time to make RPC: timeout is %v", untilDeadline)
		}
		if enc, err := encodeTimeout(untilDeadline); err == nil {
			// Tests verify that the error in encodeTimeout is unreachable, so we
			// should be safe without observability for the error case.
			md.Request.raw.Set("Grpc-Timeout", enc)
		}
	}

	body := &bytes.Buffer{}
	if err := marshalLPM(body, req, md.Spec.RequestCompression, 0 /* maxBytes */); err != nil {
		return errorf(CodeInvalidArgument, "can't marshal request as protobuf: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, body)
	if err != nil {
		return errorf(CodeInternal, "can't create HTTP request: %w", err)
	}
	request.Header = md.Request.raw

	response, err := c.doer.Do(request)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return errorf(CodeCanceled, "context canceled")
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return errorf(CodeDeadlineExceeded, "context deadline exceeded")
		}
		// Error message comes from our networking stack, so it's safe to expose.
		return wrap(CodeUnknown, err)
	}
	defer response.Body.Close()
	defer io.Copy(ioutil.Discard, response.Body)
	*md.Response = *NewImmutableHeader(response.Header)

	if response.StatusCode != http.StatusOK {
		code := CodeUnknown
		if c, ok := httpToGRPC[response.StatusCode]; ok {
			code = c
		}
		return errorf(code, "HTTP status %v", response.StatusCode)
	}
	compression := response.Header.Get("Grpc-Encoding")
	if compression == "" {
		compression = CompressionIdentity
	}
	switch compression {
	case CompressionIdentity, CompressionGzip:
	default:
		// Per https://github.com/grpc/grpc/blob/master/doc/compression.md, we
		// should return CodeInternal and specify acceptable compression(s) (in
		// addition to setting the Grpc-Accept-Encoding header).
		return errorf(CodeInternal, "unknown compression %q: accepted grpc-encoding values are %v", compression, acceptEncodingValue)
	}

	// When there's no body, errors sent from the first-party gRPC servers will
	// be in the headers.
	if err := extractError(response.Header); err != nil {
		return err
	}

	// Handling this error is a little complicated - read on.
	unmarshalErr := unmarshalLPM(response.Body, res, compression, cfg.MaxResponseBytes)
	// To ensure that we've read the trailers, read the body to completion.
	io.Copy(io.Discard, response.Body)
	serverErr := extractError(response.Trailer)
	if serverErr != nil {
		// Server sent us an error. In this case, we don't care if the
		// length-prefixed message was corrupted and unmarshalErr is non-nil.
		return serverErr
	} else if unmarshalErr != nil {
		// Server thinks response was successful, so unmarshalErr is real.
		return errorf(CodeUnknown, "server returned invalid protobuf: %w", unmarshalErr)
	}
	// Server thinks response was successful and so do we, so we're done.
	return nil
}

func extractError(h http.Header) *Error {
	codeHeader := h.Get("Grpc-Status")
	codeIsSuccess := (codeHeader == "" || codeHeader == "0")
	if codeIsSuccess {
		return nil
	}

	code, err := strconv.Atoi(codeHeader)
	if err != nil {
		return errorf(CodeUnknown, "gRPC protocol error: got invalid error code %q", codeHeader)
	}
	message := percentDecode(h.Get("Grpc-Message"))
	ret := wrap(Code(code), errors.New(message))

	detailsBinaryEncoded := h.Get("Grpc-Status-Details-Bin")
	if len(detailsBinaryEncoded) > 0 {
		detailsBinary, err := decodeBinaryHeader(detailsBinaryEncoded)
		if err != nil {
			return errorf(CodeUnknown, "server returned invalid grpc-error-details-bin trailer: %w", err)
		}
		var status statuspb.Status
		if err := proto.Unmarshal(detailsBinary, &status); err != nil {
			return errorf(CodeUnknown, "server returned invalid protobuf for error details: %w", err)
		}
		ret.details = status.Details
		// Prefer the protobuf-encoded data to the headers (grpc-go does this too).
		ret.code = Code(status.Code)
		ret.err = errors.New(status.Message)
	}

	return ret
}
