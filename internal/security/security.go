// Package security implements logflow's two independent layers of gRPC
// protection: transport encryption (TLS, optionally mutual/client-cert
// TLS) and a lightweight shared-secret bearer token checked on every call.
// They compose: TLS alone gets you encryption + server identity; adding
// client_auth gets you encryption + mutual identity; adding a token gets
// you a simple app-level access check on top of either, useful when you
// don't want to run a full certificate authority for every producer.
package security

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"logflow/internal/config"
)

// ServerTransportCredentials builds server-side gRPC transport credentials
// from a TLSConfig. A nil or disabled config returns insecure credentials,
// so every source works unencrypted by default (matching the rest of this
// project's "secure is opt-in, not a surprise migration" stance) - flip
// tls.enabled on to change that.
func ServerTransportCredentials(cfg *config.TLSConfig) (credentials.TransportCredentials, error) {
	if cfg == nil || !cfg.Enabled {
		return insecure.NewCredentials(), nil
	}
	tlsCfg, err := serverTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
}

// serverTLSConfig builds the server-side *tls.Config. The certificate (and,
// for mTLS, the client CA bundle) are re-read from disk when they change -
// see certReloader - so rotation needs no restart.
func serverTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	cr, err := newCertReloader(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
	}
	base := &tls.Config{
		MinVersion: tls.VersionTLS12,
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return cr.get(), nil
		},
	}
	if !cfg.ClientAuth {
		return base, nil
	}
	pr, err := newPoolReloader(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("load client CA: %w", err)
	}
	base.ClientAuth = tls.RequireAndVerifyClientCert
	base.ClientCAs = pr.get()
	// ClientCAs is fixed per tls.Config, so hand each handshake a fresh
	// copy carrying the current pool.
	base.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) {
		c := base.Clone()
		c.GetConfigForClient = nil
		c.ClientCAs = pr.get()
		return c, nil
	}
	return base, nil
}

// ClientTransportCredentials builds client-side gRPC transport credentials
// from a TLSConfig. A nil or disabled config returns insecure credentials.
func ClientTransportCredentials(cfg *config.TLSConfig) (credentials.TransportCredentials, error) {
	if cfg == nil || !cfg.Enabled {
		return insecure.NewCredentials(), nil
	}
	tlsCfg, err := clientTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsCfg), nil
}

// clientTLSConfig builds the client-side *tls.Config; the client
// certificate (mTLS) and the CA bundle used to verify the server are both
// reloaded on change - see certReloader / verifyServerWith.
func clientTLSConfig(cfg *config.TLSConfig) (*tls.Config, error) {
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.InsecureSkipVerify {
		tlsCfg.InsecureSkipVerify = true
	} else {
		pr, err := newPoolReloader(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("load server CA: %w", err)
		}
		// Built-in verification is disabled ONLY so verifyServerWith can
		// do the same chain + hostname check against a reloadable pool.
		tlsCfg.InsecureSkipVerify = true
		tlsCfg.VerifyConnection = verifyServerWith(pr, cfg.ServerName)
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cr, err := newCertReloader(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return cr.get(), nil
		}
	}
	if cfg.ServerName != "" {
		tlsCfg.ServerName = cfg.ServerName
	}
	return tlsCfg, nil
}

func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid certificates found in %s", path)
	}
	return pool, nil
}

// --- Bearer token auth -----------------------------------------------
//
// This is deliberately independent of TLS: it's a single shared secret
// checked via gRPC metadata on every call, for agents that want a basic
// access check without standing up a certificate authority. It is NOT a
// substitute for TLS against an untrusted network, since without TLS the
// token itself travels in clear text - see TokenPerRPCCredentials.

const tokenMetadataKey = "authorization"

// TokenPerRPCCredentials attaches a bearer token to every outgoing RPC.
// requireTLS mirrors whether the connection is actually encrypted: when
// true, grpc-go refuses to send the token over a plaintext connection,
// preventing an accidental cleartext credential leak; set it to false only
// for intentionally-insecure local testing.
type TokenPerRPCCredentials struct {
	Token      string
	RequireTLS bool
}

func (t TokenPerRPCCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{tokenMetadataKey: "Bearer " + t.Token}, nil
}

func (t TokenPerRPCCredentials) RequireTransportSecurity() bool { return t.RequireTLS }

// UnaryServerAuth returns a unary interceptor that rejects any call not
// bearing the expected bearer token.
func UnaryServerAuth(expectedToken string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkToken(ctx, expectedToken); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerAuth is the streaming-RPC equivalent of UnaryServerAuth,
// checked once at stream setup (StreamEvents is a long-lived stream, not
// a per-batch call, so per-message re-checking isn't needed).
func StreamServerAuth(expectedToken string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkToken(ss.Context(), expectedToken); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func checkToken(ctx context.Context, expected string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing authorization metadata")
	}
	vals := md.Get(tokenMetadataKey)
	if len(vals) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization token")
	}
	got := strings.TrimPrefix(vals[0], "Bearer ")
	// Constant-time comparison so a mistimed response can't leak how many
	// leading bytes of the token an attacker guessed correctly.
	if subtle.ConstantTimeCompare([]byte(got), []byte(expected)) != 1 {
		return status.Error(codes.Unauthenticated, "invalid authorization token")
	}
	return nil
}
