package daemon

import (
	"bytes"
	"fmt"

	"remote-skill/internal/proto"
)

func (c *conn) handleFileRead(env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
		Path    string `json:"path"`
		FileReadOptions
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	path, err := c.srv.resolvePath(p.Session, p.Path)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	res, err := ReadFilePath(path, p.FileReadOptions)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	c.respond(env.ID, res)
}

func (c *conn) handleFileWrite(req *request, env *proto.Envelope) {
	var p struct {
		Session       string `json:"session"`
		Path          string `json:"path"`
		Overwrite     bool   `json:"overwrite"`
		CreateParents bool   `json:"create_parents"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	path, err := c.srv.resolvePath(p.Session, p.Path)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	writer, preErr := beginAtomicWrite(path, p.Overwrite, p.CreateParents)
	if preErr != nil {
		writer = nil
	}
	var streamErr error
	defer func() {
		if writer != nil {
			writer.Abort()
		}
	}()

loop:
	for {
		select {
		case <-c.closed:
			return
		case in := <-req.in:
			switch in.Chan {
			case proto.ChIn:
				if preErr == nil && streamErr == nil && writer != nil {
					if _, err := writer.Write(in.Data); err != nil {
						streamErr = err
					}
				}
			case proto.ChEof:
				break loop
			case proto.ChError:
				streamErr = fmt.Errorf("client aborted write")
				break loop
			}
		}
	}
	if preErr != nil {
		c.fail(env.ID, asProto(preErr))
		return
	}
	if streamErr != nil {
		c.fail(env.ID, asFileError(streamErr))
		return
	}
	res, err := writer.Commit()
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	writer = nil
	c.respond(env.ID, res)
}

func (c *conn) handleFilePatch(req *request, env *proto.Envelope) {
	var p struct {
		Session string `json:"session"`
	}
	if err := env.DecodeParams(&p); err != nil {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "bad params: %v", err))
		return
	}
	var input bytes.Buffer
	tooLarge := false
	var streamErr error
loop:
	for {
		select {
		case <-c.closed:
			return
		case in := <-req.in:
			switch in.Chan {
			case proto.ChIn:
				if tooLarge {
					continue
				}
				if input.Len()+len(in.Data) > MaxPatchBytes {
					tooLarge = true
					continue
				}
				_, _ = input.Write(in.Data)
			case proto.ChEof:
				break loop
			case proto.ChError:
				streamErr = fmt.Errorf("client aborted patch")
				break loop
			}
		}
	}
	if streamErr != nil {
		c.fail(env.ID, asFileError(streamErr))
		return
	}
	if tooLarge {
		c.fail(env.ID, proto.Errorf(proto.CodeBadRequest, "patch document exceeds %d bytes", MaxPatchBytes))
		return
	}
	ops, err := ParsePatchDocument(input.Bytes())
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	resolved := make([]ResolvedPatchOperation, 0, len(ops))
	for _, op := range ops {
		path, rerr := c.srv.resolvePath(p.Session, op.Path)
		if rerr != nil {
			c.fail(env.ID, asProto(rerr))
			return
		}
		resolved = append(resolved, ResolvedPatchOperation{
			Op:            op.Op,
			Path:          path,
			Old:           op.Old,
			New:           op.New,
			Count:         op.Count,
			Content:       op.Content,
			Mode:          op.Mode,
			CreateParents: op.CreateParents,
		})
	}
	res, err := ApplyFilePatch(resolved)
	if err != nil {
		c.fail(env.ID, asProto(err))
		return
	}
	c.respond(env.ID, res)
}
