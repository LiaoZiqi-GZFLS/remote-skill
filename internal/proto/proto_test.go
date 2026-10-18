package proto

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	c := NewConn(&buf, &buf)

	req, err := NewRequest("1", MethodExec, map[string]string{"session": "main", "cmd": "ls -la"})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(req); err != nil {
		t.Fatal(err)
	}
	ev := NewEvent("1", ChOut, []byte("hello \x00 world"))
	if err := c.Write(ev); err != nil {
		t.Fatal(err)
	}
	res := NewError("1", &Error{Code: CodeBusy, Msg: "session busy"})
	if err := c.Write(res); err != nil {
		t.Fatal(err)
	}

	got, err := c.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got.Method != MethodExec || got.ID != "1" {
		t.Fatalf("unexpected envelope: %+v", got)
	}
	var p struct {
		Session string `json:"session"`
		Cmd     string `json:"cmd"`
	}
	if err := got.DecodeParams(&p); err != nil {
		t.Fatal(err)
	}
	if p.Cmd != "ls -la" || p.Session != "main" {
		t.Fatalf("params mismatch: %+v", p)
	}

	got, err = c.Read()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data, []byte("hello \x00 world")) {
		t.Fatalf("binary data mismatch: %q", got.Data)
	}

	got, err = c.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got.Err == nil || got.Err.Code != CodeBusy {
		t.Fatalf("error mismatch: %+v", got.Err)
	}

	if _, err := c.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestResultDecode(t *testing.T) {
	e, err := NewResult("7", struct {
		RC int `json:"rc"`
	}{RC: 3})
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		RC int `json:"rc"`
	}
	if err := e.DecodeResult(&out); err != nil {
		t.Fatal(err)
	}
	if out.RC != 3 {
		t.Fatalf("rc = %d", out.RC)
	}
	if string(e.Params) != "" {
		t.Fatalf("params should be empty, got %s", e.Params)
	}

	if _, err := json.Marshal(e); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidFrame(t *testing.T) {
	raw := []byte{0x7f, 0xff, 0xff, 0xff}
	c := NewConn(bytes.NewReader(raw), io.Discard)
	if _, err := c.Read(); err == nil {
		t.Fatal("expected error for oversized frame")
	}
}
