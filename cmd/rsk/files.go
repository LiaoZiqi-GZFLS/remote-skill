package main

import (
	"encoding/json"
	"fmt"
	"os"

	"remote-skill/internal/client"
	"remote-skill/internal/daemon"
)

func cmdRead(args []string) error {
	cf := newCmdFlags("read")
	var hf hostFlags
	hf.bind(cf.fs)
	offset := cf.fs.Int("offset", 1, "1-based first line")
	limit := cf.fs.Int("limit", daemon.DefaultReadLineLimit, "maximum lines to return (0 means no line limit)")
	maxBytes := cf.fs.Int64("max-bytes", daemon.DefaultReadMaxBytes, "maximum content bytes to return")
	metadataOnly := cf.fs.Bool("metadata-only", false, "return metadata without file content")
	asJSON := cf.fs.Bool("json", false, "machine readable output")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: rsk read <host> <path> [--offset N] [--limit N] [--max-bytes N]")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.ReadFile(ctx, pos[1], daemon.FileReadOptions{
		Offset:       *offset,
		Limit:        *limit,
		MaxBytes:     *maxBytes,
		MetadataOnly: *metadataOnly,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	if *metadataOnly {
		fmt.Printf("path    %s\n", res.Path)
		fmt.Printf("size    %d\n", res.Size)
		fmt.Printf("mode    %s\n", res.ModeString)
		fmt.Printf("mtime   %s\n", res.Mtime.Format("2006-01-02 15:04:05.999999999Z07:00"))
		fmt.Printf("symlink %t\n", res.Symlink)
		fmt.Printf("binary  %t\n", res.Binary)
		return nil
	}
	fmt.Print(res.Content)
	if res.Truncated {
		fmt.Fprintf(os.Stderr, "\nrsk: read truncated; continue with --offset %d\n", res.NextOffset)
	}
	return nil
}

func cmdWrite(args []string) error {
	cf := newCmdFlags("write")
	var hf hostFlags
	hf.bind(cf.fs)
	fromStdin := cf.fs.Bool("stdin", false, "read file content from stdin")
	overwrite := cf.fs.Bool("overwrite", false, "replace an existing file")
	createParents := cf.fs.Bool("create-parents", false, "create missing parent directories")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("usage: rsk write <host> <path> --stdin [--overwrite] [--create-parents]")
	}
	if !*fromStdin {
		return fmt.Errorf("write reads content from stdin; pass --stdin")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.WriteFile(ctx, pos[1], *overwrite, *createParents, os.Stdin)
	if err != nil {
		return err
	}
	action := "created"
	if res.Overwritten {
		action = "replaced"
	}
	fmt.Printf("%s %s (%d bytes)\n", action, res.Path, res.Bytes)
	return nil
}

func cmdPatch(args []string) error {
	cf := newCmdFlags("patch")
	var hf hostFlags
	hf.bind(cf.fs)
	fromStdin := cf.fs.Bool("stdin", false, "read the JSON patch document from stdin")
	asJSON := cf.fs.Bool("json", false, "machine readable output")
	pos, err := cf.parse(args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("usage: rsk patch <host> --stdin [--json]")
	}
	if !*fromStdin {
		return fmt.Errorf("patch reads a JSON document from stdin; pass --stdin")
	}
	ctx, cancel := withSignalContext()
	defer cancel()
	cfg, err := client.LoadConfig()
	if err != nil {
		return err
	}
	c, err := newClient(ctx, cfg, &hf, pos[0], hf.connectOptions())
	if err != nil {
		return err
	}
	defer c.Close()
	res, err := c.PatchFiles(ctx, os.Stdin)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	for _, change := range res.Changes {
		switch change.Action {
		case "created":
			fmt.Printf("created %s (%d bytes)\n", change.Path, change.Bytes)
		case "deleted":
			fmt.Printf("deleted %s\n", change.Path)
		default:
			if change.Replacements > 0 {
				fmt.Printf("replaced %s (%d match(es), %d bytes)\n", change.Path, change.Replacements, change.Bytes)
			} else {
				fmt.Printf("replaced %s (%d bytes)\n", change.Path, change.Bytes)
			}
		}
	}
	return nil
}
