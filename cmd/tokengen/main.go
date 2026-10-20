// tokengen mints a per-caller bearer token for a source's auth_tokens_file.
// The secret is printed once on stdout's first line; only its sha256 goes in
// the YAML entry, so the token file never holds a usable credential.
package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"logflow/internal/security"
)

func main() {
	id := flag.String("id", "", "caller id (required)")
	ttl := flag.Duration("expires-in", 0, "optional lifetime, e.g. 720h (0 = never expires)")
	flag.Parse()
	if *id == "" {
		log.Fatal("-id is required")
	}
	secret, err := security.GenerateToken()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("secret (give to the caller as its auth_token): %s\n", secret)
	fmt.Println("add to auth_tokens_file:")
	fmt.Printf("  - id: %s\n    token_sha256: %s\n", *id, security.HashToken(secret))
	if *ttl > 0 {
		fmt.Printf("    expires_at: %s\n", time.Now().Add(*ttl).UTC().Format(time.RFC3339))
	}
}
