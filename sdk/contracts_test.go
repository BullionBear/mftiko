package sdk

import (
	"bytes"
	"os"
	"slices"
	"testing"
)

func TestContractsCopy(t *testing.T) {
	got := Contracts()
	if !slices.Equal(got, helloContracts) {
		t.Fatalf("Contracts() = %q, helloContracts %q", got, helloContracts)
	}
	got[0] = "mutated"
	if helloContracts[0] == "mutated" {
		t.Fatal("Contracts returned the internal slice")
	}
	if again := Contracts(); again[0] == "mutated" {
		t.Fatal("a later Contracts saw the caller's write")
	}
}

func TestContractsMarkerInBinary(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(contractsMarker)) {
		t.Fatalf("binary %s has no %q", exe, contractsMarker)
	}
}
