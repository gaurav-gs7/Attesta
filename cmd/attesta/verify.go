package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	attestav1 "github.com/gaurav-gs7/attesta/api/v1"
)

var errChainBroken = errors.New("decision chain verification failed")

// verify checks RolloutDecision chains read from kubectl's JSON output, so
// records can be verified anywhere they were exported to, without cluster
// access:
//
//	kubectl get rolloutguards,rolloutdecisions -n payments -o json | attesta verify
//
// Guards in the input supply each chain's head (status.lastDecision), which
// also catches records deleted from the end; without the guard, a chain is
// checked from its first record to its last.
func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	file := fs.String("f", "-", "kubectl JSON (a List, or one object) to read; - is stdin")
	keyFile := fs.String("public-key", "", "PEM Ed25519 public key(s); every record must be signed by one of them")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var keys attestav1.Keys
	if *keyFile != "" {
		raw, err := os.ReadFile(*keyFile)
		if err != nil {
			return err
		}
		if keys, err = attestav1.ParsePublicKeys(raw); err != nil {
			return fmt.Errorf("--public-key: %w", err)
		}
	}
	if fs.NArg() > 0 {
		return errUsage
	}
	in := io.Reader(os.Stdin)
	if *file != "-" {
		f, err := os.Open(*file)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		in = f
	}
	raw, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	return verifyChains(raw, stdout, keys)
}

type chainKey struct{ namespace, guard, uid string }

func verifyChains(raw []byte, out io.Writer, trusted attestav1.Keys) error {
	var doc struct {
		Kind  string            `json:"kind"`
		Items []json.RawMessage `json:"items"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return fmt.Errorf("read input: %w", err)
	}
	items := doc.Items
	if doc.Items == nil {
		items = []json.RawMessage{raw}
	}

	chains := map[chainKey][]attestav1.RolloutDecision{}
	heads := map[chainKey]*attestav1.DecisionRef{}
	for _, item := range items {
		var meta struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal(item, &meta); err != nil {
			return fmt.Errorf("read input: %w", err)
		}
		switch meta.Kind {
		case "RolloutDecision":
			var d attestav1.RolloutDecision
			if err := json.Unmarshal(item, &d); err != nil {
				return fmt.Errorf("read decision: %w", err)
			}
			k := chainKey{d.Namespace, d.Spec.Guard, d.Spec.GuardUID}
			chains[k] = append(chains[k], d)
		case "RolloutGuard":
			var g attestav1.RolloutGuard
			if err := json.Unmarshal(item, &g); err != nil {
				return fmt.Errorf("read guard: %w", err)
			}
			k := chainKey{g.Namespace, g.Name, string(g.UID)}
			heads[k] = g.Status.LastDecision
			if _, ok := chains[k]; !ok {
				chains[k] = nil
			}
		}
	}
	if len(chains) == 0 {
		return errors.New("no RolloutDecisions or RolloutGuards in the input")
	}

	keys := make([]chainKey, 0, len(chains))
	for k := range chains {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.namespace != b.namespace {
			return a.namespace < b.namespace
		}
		if a.guard != b.guard {
			return a.guard < b.guard
		}
		return a.uid < b.uid
	})

	broken := 0
	for _, k := range keys {
		records := chains[k]
		head, guardSeen := heads[k]
		if !guardSeen {
			head = nil
		}
		verdict := fmt.Sprintf("OK      %s/%s: %d decisions", k.namespace, k.guard, len(records))
		err := attestav1.VerifyChain(k.guard, k.uid, records, head)
		if err == nil && trusted != nil {
			err = attestav1.VerifySignatures(k.guard, records, trusted)
		}
		if err != nil {
			broken++
			verdict = fmt.Sprintf("BROKEN  %s/%s: %v", k.namespace, k.guard, err)
		} else if !guardSeen {
			verdict += " (guard not in the input: records deleted from the end would not show)"
		}
		_, _ = fmt.Fprintln(out, verdict)
		sort.Slice(records, func(i, j int) bool { return records[i].Spec.Sequence < records[j].Spec.Sequence })
		if len(records) > 0 {
			tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
			_, _ = fmt.Fprintln(tw, "  SEQ\tDECIDED\tACTION\tTRIGGER\tREVISION\tHASH")
			for _, r := range records {
				s := r.Spec
				_, _ = fmt.Fprintf(tw, "  %d\t%s\t%s\t%s\t%s\t%.12s\n", s.Sequence, s.DecidedAt, s.Action, s.Trigger, s.Deployment.Revision, s.Hash)
			}
			_ = tw.Flush()
		}
	}
	if broken > 0 {
		return fmt.Errorf("%w: %d of %d chains", errChainBroken, broken, len(keys))
	}
	return nil
}
