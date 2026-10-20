package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// reloadCheckInterval throttles how often a handshake re-stats the
// certificate files. A var, not a const, so tests can shrink it.
var reloadCheckInterval = time.Second

type fileStamp struct {
	mod  time.Time
	size int64
}

func stamp(path string) (fileStamp, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return fileStamp{}, err
	}
	return fileStamp{fi.ModTime(), fi.Size()}, nil
}

// certReloader serves the current cert/key pair, transparently re-reading
// the files when either changes on disk (mtime or size) - so rotating a
// certificate (cert-manager, certbot, a secret volume update) takes effect
// for every NEW handshake without restarting the agent. Connections
// already established keep the certificate they were negotiated with
// until they reconnect (standard TLS behavior - a long-lived gRPC stream
// is not renegotiated).
//
// A reload that fails (half-written file, cert/key mismatch mid-rotation)
// keeps serving the last good certificate and is retried on the next
// check, rather than breaking every handshake until someone notices.
type certReloader struct {
	certFile, keyFile string

	mu        sync.Mutex
	cert      *tls.Certificate
	stC, stK  fileStamp
	lastCheck time.Time
}

func newCertReloader(certFile, keyFile string) (*certReloader, error) {
	r := &certReloader{certFile: certFile, keyFile: keyFile}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *certReloader) load() error {
	stC, err := stamp(r.certFile)
	if err != nil {
		return err
	}
	stK, err := stamp(r.keyFile)
	if err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return err
	}
	r.cert, r.stC, r.stK = &cert, stC, stK
	return nil
}

// get returns the current certificate, reloading first if the files changed.
func (r *certReloader) get() *tls.Certificate {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.lastCheck) >= reloadCheckInterval {
		r.lastCheck = time.Now()
		stC, errC := stamp(r.certFile)
		stK, errK := stamp(r.keyFile)
		if errC == nil && errK == nil && (stC != r.stC || stK != r.stK) {
			if err := r.load(); err != nil {
				log.Printf("security: reloading certificate %s failed (keeping the previous one, will retry): %v", r.certFile, err)
			} else {
				log.Printf("security: reloaded certificate %s", r.certFile)
			}
		}
	}
	return r.cert
}

// poolReloader is certReloader's equivalent for a CA bundle.
type poolReloader struct {
	path string

	mu        sync.Mutex
	pool      *x509.CertPool
	st        fileStamp
	lastCheck time.Time
}

func newPoolReloader(path string) (*poolReloader, error) {
	r := &poolReloader{path: path}
	if err := r.load(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *poolReloader) load() error {
	st, err := stamp(r.path)
	if err != nil {
		return err
	}
	pool, err := loadCAPool(r.path)
	if err != nil {
		return err
	}
	r.pool, r.st = pool, st
	return nil
}

func (r *poolReloader) get() *x509.CertPool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if time.Since(r.lastCheck) >= reloadCheckInterval {
		r.lastCheck = time.Now()
		if st, err := stamp(r.path); err == nil && st != r.st {
			if err := r.load(); err != nil {
				log.Printf("security: reloading CA bundle %s failed (keeping the previous one, will retry): %v", r.path, err)
			} else {
				log.Printf("security: reloaded CA bundle %s", r.path)
			}
		}
	}
	return r.pool
}

// verifyServerWith returns a tls.Config.VerifyConnection that validates the
// server's certificate chain against the CURRENT contents of the CA
// bundle. tls.Config.RootCAs is fixed at construction, so a reloadable CA
// bundle on the client side needs custom verification (the pattern Go's
// crypto/tls documents for InsecureSkipVerify + VerifyConnection): the
// caller sets InsecureSkipVerify (disabling only the built-in check) and
// this re-implements it - chain verification with intermediates, and the
// hostname check against serverName (or the connection's own SNI).
func verifyServerWith(pr *poolReloader, serverName string) func(tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("server presented no certificate")
		}
		name := serverName
		if name == "" {
			name = cs.ServerName
		}
		opts := x509.VerifyOptions{
			Roots:         pr.get(),
			DNSName:       name,
			Intermediates: x509.NewCertPool(),
		}
		for _, c := range cs.PeerCertificates[1:] {
			opts.Intermediates.AddCert(c)
		}
		_, err := cs.PeerCertificates[0].Verify(opts)
		return err
	}
}
