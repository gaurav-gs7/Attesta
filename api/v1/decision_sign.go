package v1

import (
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"sort"
)

// SignedMessage is what a decision's signature covers: its hash, under a
// prefix that keeps the signature from meaning anything elsewhere.
func SignedMessage(hash string) []byte {
	return []byte("attesta.io/v1 RolloutDecision " + hash)
}

// KeyID names a public key: the first 16 hex characters of its SHA-256.
func KeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:])[:16]
}

// Sign signs the sealed spec's hash. Seal first: resealing clears it.
func (s *RolloutDecisionSpec) Sign(key ed25519.PrivateKey) error {
	if s.Hash == "" {
		return errors.New("sign an unsealed decision")
	}
	pub, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		return errors.New("not an Ed25519 key")
	}
	s.Signature = &DecisionSignature{
		KeyID: KeyID(pub),
		Value: base64.StdEncoding.EncodeToString(ed25519.Sign(key, SignedMessage(s.Hash))),
	}
	return nil
}

// Keys are trusted public keys by KeyID.
type Keys map[string]ed25519.PublicKey

// Add trusts a key.
func (k Keys) Add(pub ed25519.PublicKey) { k[KeyID(pub)] = pub }

// IDs lists the trusted key IDs, sorted.
func (k Keys) IDs() []string {
	ids := make([]string, 0, len(k))
	for id := range k {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// VerifySignature reports whether the spec carries a valid signature of its
// hash by one of keys. The hash itself is checked by VerifyChain.
func (s RolloutDecisionSpec) VerifySignature(keys Keys) error {
	if s.Signature == nil {
		return errors.New("unsigned")
	}
	pub, ok := keys[s.Signature.KeyID]
	if !ok {
		return fmt.Errorf("signed by unknown key %s", s.Signature.KeyID)
	}
	sig, err := base64.StdEncoding.DecodeString(s.Signature.Value)
	if err != nil || !ed25519.Verify(pub, SignedMessage(s.Hash), sig) {
		return fmt.Errorf("signature by key %s does not verify", s.Signature.KeyID)
	}
	return nil
}

// VerifySignatures checks every record's signature. Call VerifyChain too:
// a signature vouches for a hash, the chain for the contents behind it.
func VerifySignatures(guard string, records []RolloutDecision, keys Keys) error {
	sorted := append([]RolloutDecision(nil), records...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Spec.Sequence < sorted[j].Spec.Sequence })
	for _, r := range sorted {
		if err := r.Spec.VerifySignature(keys); err != nil {
			return &ChainError{Guard: guard, Sequence: r.Spec.Sequence, Problem: fmt.Sprintf("record %s: %v", r.Name, err)}
		}
	}
	return nil
}

// ParsePrivateKey reads a PEM PKCS#8 Ed25519 private key, as written by
// `openssl genpkey -algorithm ed25519`.
func ParsePrivateKey(pemBytes []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	ed, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("a %T, not an Ed25519 key", key)
	}
	return ed, nil
}

// ParsePublicKeys reads one or more PEM PKIX Ed25519 public keys, as
// written by `openssl pkey -pubout`. Several keys let a verifier accept
// records signed before and after a key rotation.
func ParsePublicKeys(pemBytes []byte) (Keys, error) {
	keys := Keys{}
	for {
		var block *pem.Block
		block, pemBytes = pem.Decode(pemBytes)
		if block == nil {
			break
		}
		key, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		ed, ok := key.(ed25519.PublicKey)
		if !ok {
			return nil, fmt.Errorf("a %T, not an Ed25519 key", key)
		}
		keys.Add(ed)
	}
	if len(keys) == 0 {
		return nil, errors.New("no PEM public key")
	}
	return keys, nil
}
