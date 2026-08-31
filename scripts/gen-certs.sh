#!/usr/bin/env bash
# Generates a throwaway CA plus a server certificate (for agent gRPC
# sources) and a client certificate (for mTLS-authenticated sinks), all
# valid for "localhost"/127.0.0.1, under ./certs. For local testing and
# demos only - do not reuse these in anything reachable from an untrusted
# network.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p certs
cd certs

echo "== generating CA =="
openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -keyout ca-key.pem -out ca-cert.pem \
  -subj "/CN=logflow-demo-ca" 2>/dev/null

echo "== generating server cert (CN=localhost) =="
openssl req -newkey rsa:2048 -nodes -keyout server-key.pem -out server-req.pem \
  -subj "/CN=localhost" 2>/dev/null
openssl x509 -req -in server-req.pem -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out server-cert.pem -days 3650 \
  -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") 2>/dev/null

echo "== generating client cert (CN=logflow-client, for mTLS) =="
openssl req -newkey rsa:2048 -nodes -keyout client-key.pem -out client-req.pem \
  -subj "/CN=logflow-client" 2>/dev/null
openssl x509 -req -in client-req.pem -CA ca-cert.pem -CAkey ca-key.pem \
  -CAcreateserial -out client-cert.pem -days 3650 2>/dev/null

rm -f server-req.pem client-req.pem ca-cert.srl
echo "== done: certs/{ca-cert,server-cert,server-key,client-cert,client-key}.pem =="
