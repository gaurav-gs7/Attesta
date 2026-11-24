package v1

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"testing"
)

func TestSignaturesVerifyOnlyWithTheRightKeyAndHash(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	otherPub, otherPriv, _ := ed25519.GenerateKey(nil)
	keys := Keys{}
	keys.Add(pub)

	records := chain(t, 3)
	for i := range records {
		if err := records[i].Spec.Sign(priv); err != nil {
			t.Fatal(err)
		}
	}
	if err := VerifySignatures("g", records, keys); err != nil {
		t.Fatal(err)
	}
	// Signing does not change the hash, so the chain still verifies.
	if err := VerifyChain(records[0].Spec.Guard, records[0].Spec.GuardUID, records, headOf(records)); err != nil {
		t.Fatal(err)
	}

	forged := append([]RolloutDecision(nil), records...)
	forged[1].Spec.Action = ActionPromoted
	forged[1].Spec.Hash, _ = forged[1].Spec.ComputeHash() // rehashed, but cannot be re-signed
	var chainErr *ChainError
	if err := VerifySignatures("g", forged, keys); !errors.As(err, &chainErr) || chainErr.Sequence != 2 || !strings.Contains(err.Error(), "does not verify") {
		t.Fatalf("rehashed record: %v", err)
	}

	resigned := append([]RolloutDecision(nil), records...)
	_ = resigned[2].Spec.Sign(otherPriv)
	if err := VerifySignatures("g", resigned, keys); err == nil || !strings.Contains(err.Error(), "unknown key "+KeyID(otherPub)) {
		t.Fatalf("record signed by another key: %v", err)
	}

	unsigned := append([]RolloutDecision(nil), records...)
	unsigned[0].Spec.Signature = nil
	if err := VerifySignatures("g", unsigned, keys); err == nil || !strings.Contains(err.Error(), "unsigned") {
		t.Fatalf("unsigned record: %v", err)
	}
}

func TestKeysParseFromOpenSSLPEM(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	parsed, err := ParsePrivateKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil || !parsed.Equal(priv) {
		t.Fatalf("private key: %v", err)
	}
	pub2, _, _ := ed25519.GenerateKey(nil)
	var bundle []byte
	for _, p := range []ed25519.PublicKey{pub, pub2} {
		der, _ := x509.MarshalPKIXPublicKey(p)
		bundle = append(bundle, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})...)
	}
	keys, err := ParsePublicKeys(bundle)
	if err != nil || len(keys) != 2 || keys[KeyID(pub)] == nil {
		t.Fatalf("public keys: %v %v", keys.IDs(), err)
	}
	if _, err := ParsePublicKeys([]byte("nope")); err == nil {
		t.Fatal("garbage parsed as a key")
	}
}
