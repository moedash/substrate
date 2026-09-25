// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actorflow

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestWorkerOptionsCarryTheStopTimeout covers the one worker option that has to
// be set. At the SDK's default of zero a shutdown cancels the activity context
// at once, cutting off a step that holds the actor's lease and leaving the
// record in a transitional state for the next pod to crash out of.
func TestWorkerOptionsCarryTheStopTimeout(t *testing.T) {
	want := 2 * time.Minute
	if got := workerOptions(Config{StopTimeout: want}).WorkerStopTimeout; got != want {
		t.Errorf("WorkerStopTimeout = %v, want %v", got, want)
	}
}

// TestWorkerOptionsCapConcurrentSteps covers the cap on steps one pod runs at
// once, without which a backlog after an outage becomes a stampede against the
// store and atelet.
func TestWorkerOptionsCapConcurrentSteps(t *testing.T) {
	if got := workerOptions(Config{MaxConcurrentSteps: 32}).MaxConcurrentActivityExecutionSize; got != 32 {
		t.Errorf("MaxConcurrentActivityExecutionSize = %d, want 32", got)
	}
}

// TestClientOptionsDialPlaintextByDefault covers the dev-server case: nothing
// configured means no TLS and no credential.
func TestClientOptionsDialPlaintextByDefault(t *testing.T) {
	opts, err := clientOptions(Config{Address: "localhost:7233", Namespace: "default"}, slog.Default())
	if err != nil {
		t.Fatalf("clientOptions = %v, want nil", err)
	}
	if opts.ConnectionOptions.TLS != nil {
		t.Error("TLS configured, want plaintext when nothing asks for TLS")
	}
	if opts.Credentials != nil {
		t.Error("Credentials set, want none without an API key")
	}
	if opts.HostPort != "localhost:7233" || opts.Namespace != "default" {
		t.Errorf("HostPort, Namespace = %q, %q, want the configured ones", opts.HostPort, opts.Namespace)
	}
}

// TestClientOptionsWithAnAPIKey covers Temporal Cloud: the key is sent as a
// credential, and TLS is on without being asked for, because the key travels
// over nothing else.
func TestClientOptionsWithAnAPIKey(t *testing.T) {
	opts, err := clientOptions(Config{Address: "ns.tmprl.cloud:7233", APIKey: "secret"}, slog.Default())
	if err != nil {
		t.Fatalf("clientOptions = %v, want nil", err)
	}
	if opts.Credentials == nil {
		t.Error("Credentials = nil, want the API key")
	}
	if opts.ConnectionOptions.TLS == nil {
		t.Error("TLS = nil, want it implied by the API key")
	}
}

// TestClientOptionsWithTLSFiles covers a secured self-hosted frontend: the CA
// bundle replaces the system roots, and the client bundle is presented on
// request.
func TestClientOptionsWithTLSFiles(t *testing.T) {
	caFile := writeTestCA(t)
	cfg := Config{
		Address: "temporal-frontend:7233",
		TLS:     TLSOptions{CACerts: caFile, ClientCredBundle: filepath.Join(t.TempDir(), "client.pem")},
	}
	opts, err := clientOptions(cfg, slog.Default())
	if err != nil {
		t.Fatalf("clientOptions = %v, want nil", err)
	}
	tlsCfg := opts.ConnectionOptions.TLS
	if tlsCfg == nil {
		t.Fatal("TLS = nil, want it implied by the CA bundle")
	}
	if tlsCfg.RootCAs == nil {
		t.Error("RootCAs = nil, want the CA bundle")
	}
	if tlsCfg.GetClientCertificate == nil {
		t.Error("GetClientCertificate = nil, want the client bundle loader")
	}
	if opts.Credentials != nil {
		t.Error("Credentials set, want none without an API key")
	}
}

// TestClientOptionsWithTLSAlone covers a frontend behind a public certificate:
// TLS with the system roots and nothing else.
func TestClientOptionsWithTLSAlone(t *testing.T) {
	opts, err := clientOptions(Config{TLS: TLSOptions{Enabled: true}}, slog.Default())
	if err != nil {
		t.Fatalf("clientOptions = %v, want nil", err)
	}
	tlsCfg := opts.ConnectionOptions.TLS
	if tlsCfg == nil || tlsCfg.RootCAs != nil || tlsCfg.GetClientCertificate != nil {
		t.Errorf("TLS = %+v, want TLS on with the system roots and no client certificate", tlsCfg)
	}
}

// TestClientOptionsRefuseAnUnreadableCABundle covers the failure that has to
// stop startup: a CA bundle that is missing or holds no certificate would
// otherwise fall back to the system roots without saying so.
func TestClientOptionsRefuseAnUnreadableCABundle(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.pem")
	if _, err := clientOptions(Config{TLS: TLSOptions{CACerts: missing}}, slog.Default()); err == nil {
		t.Error("clientOptions with a missing CA bundle = nil, want an error")
	}

	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := clientOptions(Config{TLS: TLSOptions{CACerts: empty}}, slog.Default()); err == nil {
		t.Error("clientOptions with a CA bundle holding no certificate = nil, want an error")
	}
}

// writeTestCA writes a self-signed certificate and returns its path.
func writeTestCA(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
