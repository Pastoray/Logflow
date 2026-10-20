package security

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"logflow/internal/config"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  []byte
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return &testCA{c, key, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}
}

// issue returns PEM cert+key for a leaf with the given serial, valid for
// localhost and usable as both server and client cert.
func (ca *testCA) issue(t *testing.T, serial int64) (certPEM, keyPEM []byte) {
	t.Helper()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "localhost"},
		DNSNames:  []string{"localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	kder, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder})
}

func write(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	// Guarantee a distinct mtime even on coarse-granularity filesystems.
	future := time.Now().Add(time.Duration(len(data)%7+1) * time.Second)
	_ = os.Chtimes(path, future, future)
}

func fastReload(t *testing.T) {
	old := reloadCheckInterval
	reloadCheckInterval = 0
	t.Cleanup(func() { reloadCheckInterval = old })
}

// handshake runs one real TLS handshake between serverCfg and clientCfg
// over loopback and returns the server's presented leaf serial (as seen by
// the client) and the client's presented leaf serial (as seen by the
// server, 0 if none), or the first handshake error.
func handshake(t *testing.T, serverCfg, clientCfg *tls.Config) (srvSerial, cliSerial int64, err error) {
	t.Helper()
	ln, lerr := tls.Listen("tcp", "127.0.0.1:0", serverCfg)
	if lerr != nil {
		t.Fatal(lerr)
	}
	defer ln.Close()
	type res struct {
		serial int64
		err    error
	}
	ch := make(chan res, 1)
	go func() {
		c, aerr := ln.Accept()
		if aerr != nil {
			ch <- res{0, aerr}
			return
		}
		defer c.Close()
		tc := c.(*tls.Conn)
		if herr := tc.Handshake(); herr != nil {
			ch <- res{0, herr}
			return
		}
		var s int64
		if pc := tc.ConnectionState().PeerCertificates; len(pc) > 0 {
			s = pc[0].SerialNumber.Int64()
		}
		ch <- res{s, nil}
	}()
	cc := clientCfg.Clone()
	if cc.ServerName == "" {
		cc.ServerName = "localhost"
	}
	conn, derr := tls.Dial("tcp", ln.Addr().String(), cc)
	if derr != nil {
		<-ch
		return 0, 0, derr
	}
	defer conn.Close()
	// TLS 1.3: server-side client-cert rejection surfaces on first read.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Read(make([]byte, 1))
	r := <-ch
	if r.err != nil {
		return 0, 0, r.err
	}
	return conn.ConnectionState().PeerCertificates[0].SerialNumber.Int64(), r.serial, nil
}

var _ = net.IPv4len

func TestServerCertRotation(t *testing.T) {
	fastReload(t)
	dir := t.TempDir()
	ca := newTestCA(t, "ca")
	caFile := filepath.Join(dir, "ca.pem")
	write(t, caFile, ca.pem)
	certFile, keyFile := filepath.Join(dir, "srv.pem"), filepath.Join(dir, "srv.key")
	c1, k1 := ca.issue(t, 101)
	write(t, certFile, c1)
	write(t, keyFile, k1)

	srv, err := serverTLSConfig(&config.TLSConfig{Enabled: true, CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatal(err)
	}
	cli, err := clientTLSConfig(&config.TLSConfig{Enabled: true, CAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	if s, _, err := handshake(t, srv, cli); err != nil || s != 101 {
		t.Fatalf("before rotation: serial=%d err=%v, want 101", s, err)
	}

	// Rotate to a new cert (same CA) - NO new config built, no restart.
	c2, k2 := ca.issue(t, 102)
	write(t, certFile, c2)
	write(t, keyFile, k2)
	if s, _, err := handshake(t, srv, cli); err != nil || s != 102 {
		t.Fatalf("after rotation: serial=%d err=%v, want 102", s, err)
	}
}

func TestBadRotationKeepsLastGoodCert(t *testing.T) {
	fastReload(t)
	dir := t.TempDir()
	ca := newTestCA(t, "ca")
	caFile := filepath.Join(dir, "ca.pem")
	write(t, caFile, ca.pem)
	certFile, keyFile := filepath.Join(dir, "srv.pem"), filepath.Join(dir, "srv.key")
	c1, k1 := ca.issue(t, 201)
	write(t, certFile, c1)
	write(t, keyFile, k1)
	srv, _ := serverTLSConfig(&config.TLSConfig{Enabled: true, CertFile: certFile, KeyFile: keyFile})
	cli, _ := clientTLSConfig(&config.TLSConfig{Enabled: true, CAFile: caFile})

	// Simulate a torn rotation: new cert written, key still the old (mismatch).
	c2, k2 := ca.issue(t, 202)
	write(t, certFile, c2)
	if s, _, err := handshake(t, srv, cli); err != nil || s != 201 {
		t.Fatalf("during bad rotation: serial=%d err=%v, want the last good cert (201)", s, err)
	}
	// Rotation completes: key now matches - picked up on the next handshake.
	write(t, keyFile, k2)
	if s, _, err := handshake(t, srv, cli); err != nil || s != 202 {
		t.Fatalf("after fixing rotation: serial=%d err=%v, want 202", s, err)
	}
}

func TestMTLSClientCertAndClientCARotation(t *testing.T) {
	fastReload(t)
	dir := t.TempDir()
	ca := newTestCA(t, "ca")
	caFile := filepath.Join(dir, "ca.pem")
	write(t, caFile, ca.pem)
	sc, sk := ca.issue(t, 300)
	srvCert, srvKey := filepath.Join(dir, "srv.pem"), filepath.Join(dir, "srv.key")
	write(t, srvCert, sc)
	write(t, srvKey, sk)
	cc1, ck1 := ca.issue(t, 301)
	cliCert, cliKey := filepath.Join(dir, "cli.pem"), filepath.Join(dir, "cli.key")
	write(t, cliCert, cc1)
	write(t, cliKey, ck1)

	srv, err := serverTLSConfig(&config.TLSConfig{Enabled: true, CertFile: srvCert, KeyFile: srvKey, CAFile: caFile, ClientAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	cli, err := clientTLSConfig(&config.TLSConfig{Enabled: true, CAFile: caFile, CertFile: cliCert, KeyFile: cliKey})
	if err != nil {
		t.Fatal(err)
	}
	if _, c, err := handshake(t, srv, cli); err != nil || c != 301 {
		t.Fatalf("initial mTLS: client serial=%d err=%v, want 301", c, err)
	}

	// Client cert rotation (same CA).
	cc2, ck2 := ca.issue(t, 302)
	write(t, cliCert, cc2)
	write(t, cliKey, ck2)
	if _, c, err := handshake(t, srv, cli); err != nil || c != 302 {
		t.Fatalf("after client cert rotation: client serial=%d err=%v, want 302", c, err)
	}

	// CA rotation: a client cert from a NEW CA is rejected until the
	// server's client-CA file is updated to trust it - no restart.
	ca2 := newTestCA(t, "ca2")
	cc3, ck3 := ca2.issue(t, 303)
	write(t, cliCert, cc3)
	write(t, cliKey, ck3)
	if _, _, err := handshake(t, srv, cli); err == nil {
		t.Fatal("client cert from an untrusted CA was accepted")
	}
	write(t, caFile, append(append([]byte{}, ca.pem...), ca2.pem...))
	if _, c, err := handshake(t, srv, cli); err != nil || c != 303 {
		t.Fatalf("after server client-CA rotation: client serial=%d err=%v, want 303", c, err)
	}
}

func TestClientCARotationAndVerificationStillStrict(t *testing.T) {
	fastReload(t)
	dir := t.TempDir()
	ca1, ca2 := newTestCA(t, "ca1"), newTestCA(t, "ca2")
	srvCert, srvKey := filepath.Join(dir, "srv.pem"), filepath.Join(dir, "srv.key")
	sc, sk := ca2.issue(t, 400) // server cert signed by CA2
	write(t, srvCert, sc)
	write(t, srvKey, sk)
	srv, _ := serverTLSConfig(&config.TLSConfig{Enabled: true, CertFile: srvCert, KeyFile: srvKey})

	clientCA := filepath.Join(dir, "client-ca.pem")
	write(t, clientCA, ca1.pem) // client only trusts CA1
	cli, err := clientTLSConfig(&config.TLSConfig{Enabled: true, CAFile: clientCA})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := handshake(t, srv, cli); err == nil {
		t.Fatal("server cert from an untrusted CA was accepted by the client")
	}
	// CA bundle rotated to include CA2 - same client config now trusts it.
	write(t, clientCA, append(append([]byte{}, ca1.pem...), ca2.pem...))
	if s, _, err := handshake(t, srv, cli); err != nil || s != 400 {
		t.Fatalf("after client CA rotation: serial=%d err=%v, want 400", s, err)
	}

	// Hostname verification must still be enforced by the custom verifier.
	wrongName, _ := clientTLSConfig(&config.TLSConfig{Enabled: true, CAFile: clientCA, ServerName: "not-localhost.example"})
	wn := wrongName.Clone()
	if _, _, err := handshake(t, srv, wn); err == nil {
		t.Fatal("certificate for 'localhost' accepted for server name 'not-localhost.example'")
	}
}
