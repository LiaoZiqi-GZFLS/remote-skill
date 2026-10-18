// Package proto implements the framed message protocol spoken between the
// local `rsk` client and the remote `rskd` daemon.
//
// Frames are a 4 byte big endian length followed by a JSON encoded Envelope.
// The transport is normally the stdin/stdout of an `ssh host rskd __bridge`
// invocation, so a single connection carries control messages and multiplexed
// data streams (command output, file transfers, port forwards).
package proto

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// APIVersion is bumped whenever the wire format changes incompatibly.
const APIVersion = 1

// MaxFrameSize caps a single frame. Command output and file chunks are sent
// as separate frames, so this only limits a single message.
const MaxFrameSize = 32 << 20

// Envelope types.
const (
	TypeHello   = "hello"
	TypeWelcome = "welcome"
	TypeReq     = "req"
	TypeRes     = "res"
	TypeEvent   = "ev"
	TypeBye     = "bye"
)

// Event channels, used with TypeEvent.
const (
	ChOut    = "out"    // daemon -> client: session output
	ChIn     = "in"     // client -> daemon: session input / upload bytes
	ChEof    = "eof"    // end of a stream
	ChDone   = "done"   // terminal event for a request that streams
	ChError  = "error"  // non fatal error attached to a streaming request
	ChSignal = "signal" // interrupt/terminate a running command
	ChMeta   = "meta"   // out of band metadata
	ChOpen   = "open"   // a reverse tunnel accepted a new connection
	ChClose  = "close"  // that tunnel connection ended
)

// Method names.
const (
	MethodInfo              = "info"
	MethodPing              = "ping"
	MethodSessionOpen       = "session.open"
	MethodSessionList       = "session.list"
	MethodSessionClose      = "session.close"
	MethodSessionAttach     = "session.attach"
	MethodSessionResize     = "session.resize"
	MethodSessionSignal     = "session.signal"
	MethodSessionInfo       = "session.info"
	MethodSessionScrollback = "session.scrollback"
	MethodExec              = "exec"
	MethodExecTTY           = "exec.tty"
	MethodFilePut           = "file.put"
	MethodFileGet           = "file.get"
	MethodFileList          = "file.list"
	MethodFileManifest      = "file.manifest"
	MethodFileRemove        = "file.remove"
	MethodTunnelOpen        = "tunnel.open"
	MethodTunnelListen      = "tunnel.listen"
	MethodShutdown          = "daemon.shutdown"
)

// Error codes.
const (
	CodeBadRequest = "bad_request"
	CodeNotFound   = "not_found"
	CodeBusy       = "busy"
	CodeConflict   = "conflict"
	CodeInternal   = "internal"
	CodeIO         = "io"
	CodeTimeout    = "timeout"
	CodeClosed     = "closed"
	CodeVersion    = "version_mismatch"
)

// Error is a structured failure returned by the daemon.
type Error struct {
	Code string `json:"code"`
	Msg  string `json:"msg"`
	Hint string `json:"hint,omitempty"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Hint != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Code, e.Msg, e.Hint)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Msg)
}

// Errorf builds a structured protocol error.
func Errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Msg: fmt.Sprintf(format, args...)}
}

// WithHint attaches an actionable hint to an error.
func (e *Error) WithHint(hint string) *Error {
	e.Hint = hint
	return e
}

// Envelope is the single wire message type.
type Envelope struct {
	Type   string          `json:"t"`
	ID     string          `json:"id,omitempty"`
	Method string          `json:"m,omitempty"`
	Params json.RawMessage `json:"p,omitempty"`
	Result json.RawMessage `json:"r,omitempty"`
	Err    *Error          `json:"e,omitempty"`
	Chan   string          `json:"c,omitempty"`
	Data   []byte          `json:"d,omitempty"`
	Extra  map[string]any  `json:"x,omitempty"`
}

// NewRequest builds a request envelope.
func NewRequest(id, method string, params any) (*Envelope, error) {
	raw, err := marshal(params)
	if err != nil {
		return nil, err
	}
	return &Envelope{Type: TypeReq, ID: id, Method: method, Params: raw}, nil
}

// NewResult builds a success response envelope.
func NewResult(id string, result any) (*Envelope, error) {
	raw, err := marshal(result)
	if err != nil {
		return nil, err
	}
	return &Envelope{Type: TypeRes, ID: id, Result: raw}, nil
}

// NewError builds a failure response envelope.
func NewError(id string, err *Error) *Envelope {
	return &Envelope{Type: TypeRes, ID: id, Err: err}
}

// NewEvent builds an event envelope on the given channel.
func NewEvent(id, channel string, data []byte) *Envelope {
	return &Envelope{Type: TypeEvent, ID: id, Chan: channel, Data: data}
}

func marshal(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// DecodeParams unmarshals the envelope params into v.
func (e *Envelope) DecodeParams(v any) error {
	if len(e.Params) == 0 {
		return nil
	}
	return json.Unmarshal(e.Params, v)
}

// DecodeResult unmarshals the envelope result into v.
func (e *Envelope) DecodeResult(v any) error {
	if len(e.Result) == 0 {
		return nil
	}
	return json.Unmarshal(e.Result, v)
}

// Conn is a framed, concurrency safe envelope connection.
type Conn struct {
	r   *bufio.Reader
	w   io.Writer
	wmu sync.Mutex
}

// NewConn wraps a reader/writer pair.
func NewConn(r io.Reader, w io.Writer) *Conn {
	return &Conn{r: bufio.NewReaderSize(r, 64<<10), w: w}
}

// Read reads the next envelope. It returns io.EOF when the peer closed.
func (c *Conn) Read() (*Envelope, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > MaxFrameSize {
		return nil, fmt.Errorf("proto: invalid frame size %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(c.r, buf); err != nil {
		return nil, err
	}
	var e Envelope
	if err := json.Unmarshal(buf, &e); err != nil {
		return nil, fmt.Errorf("proto: decode envelope: %w", err)
	}
	return &e, nil
}

// Write sends an envelope, framing it.
func (c *Conn) Write(e *Envelope) error {
	buf, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if len(buf) > MaxFrameSize {
		return fmt.Errorf("proto: frame too large (%d bytes)", len(buf))
	}
	frame := make([]byte, 4+len(buf))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(buf)))
	copy(frame[4:], buf)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.w.Write(frame)
	return err
}
