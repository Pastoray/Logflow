package config

import (
	"os"
	"strings"
	"testing"
)

func TestExpandEnv(t *testing.T) {
	t.Setenv("POD_NAME", "collector-2")
	got, err := expandEnv([]byte("id: ${POD_NAME}\nx: ${NOPE:-fallback}\nre: '^a$|b${POD_NAME}$'\nbare: $HOME\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := "id: collector-2\nx: fallback\nre: '^a$|bcollector-2$'\nbare: $HOME\n"
	if string(got) != want {
		t.Fatalf("got %q\nwant %q", got, want)
	}
	os.Unsetenv("UNSET_ONE")
	if _, err := expandEnv([]byte("a: ${UNSET_ONE}")); err == nil || !strings.Contains(err.Error(), "UNSET_ONE") {
		t.Fatalf("unset var without default must fail loudly, got %v", err)
	}
	t.Setenv("EMPTY", "")
	if got, _ := expandEnv([]byte("a: [${EMPTY:-d}]")); string(got) != "a: []" {
		t.Fatalf("set-but-empty must win over the default, got %q", got)
	}
}
