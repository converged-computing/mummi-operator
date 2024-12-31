package certs

// https://go.dev/src/crypto/tls/generate_cert.go
// https://gist.github.com/shaneutt/5e1995295cff6721c89a71d13a71c251

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"time"
)

type Certs struct {
	Certificate []byte // cert.pem
	Client      []byte // client.pem
	Key         []byte // key.pem
	CA          []byte // ca_certificate.pem

	// I don't think we need this, but I'm not sure
	ServerCert *tls.Certificate
}

// generateX509 generates a server or client certificate
func generateX509(
	notValidBefore, notValidAfter time.Time,
	serialNumber *big.Int,
	subject pkix.Name,
	priv *rsa.PrivateKey,
	isClient bool,
	host string,
) ([]byte, error) {

	// ECDSA, ED25519 and RSA subject keys should have the DigitalSignature
	// KeyUsage bits set in the x509.Certificate template
	keyUsage := x509.KeyUsageDigitalSignature

	// RSA subject keys should have the KeyEncipherment KeyUsage bits set
	// For TLS this is for key exchange / auth
	keyUsage |= x509.KeyUsageKeyEncipherment

	// Create the x509 Certificate (server certificate)
	template := x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		NotBefore:             notValidBefore,
		NotAfter:              notValidAfter,
		KeyUsage:              keyUsage,
		BasicConstraintsValid: true,
	}

	// Create the x509 Certificate (client certificate)
	// Generating client cert is the same, but ExtKeyUsage should be set to x509.ExtKeyUsageClientAuth
	if isClient {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	}

	// Parse hosts
	hosts := strings.Split(host, ",")

	for _, h := range hosts {
		ip := net.ParseIP(h)
		if ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	// Generate the public key
	certBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}

	// Write the certificate (cert.pem) to a buffer
	var certOut bytes.Buffer
	err = pem.Encode(&certOut, &pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	return certOut.Bytes(), err
}

// generateCA generates the certificate authority "CA" certificate
func generateCA(
	notValidBefore, notValidAfter time.Time,
	serialNumber *big.Int,
	subject pkix.Name,
	priv *rsa.PrivateKey,
) ([]byte, error) {

	// set up our CA certificate
	ca := &x509.Certificate{
		SerialNumber:          serialNumber,
		Subject:               subject,
		NotBefore:             notValidBefore,
		NotAfter:              notValidAfter,
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	// create the CA
	caBytes, err := x509.CreateCertificate(rand.Reader, ca, ca, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}

	// PEM "Privacy Enhanced Mail" encode
	// Base64-encoded format for storing and sending cryptographic keys, certs, etc.
	caPEM := new(bytes.Buffer)
	pem.Encode(caPEM, &pem.Block{Type: "CERTIFICATE", Bytes: caBytes})
	return caPEM.Bytes(), nil
}

// Generate a self-signed X.509 certificate for a TLS server. Outputs to
func GenerateCertificates(host string) (*Certs, error) {

	// Return certificates for configmaps or secrets
	certs := Certs{}

	// Shared metadata
	subject := pkix.Name{Organization: []string{"MiniMummi"}}

	// Set hard coded variables
	// Valid for a year...
	// The certificate is valid from... now into the future
	validDuration := 365 * 24 * time.Hour
	notValidBefore := time.Now()
	notValidAfter := notValidBefore.Add(validDuration)

	// Generate a serial number
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return &certs, err
	}

	// Generate private RSA key, size 2048 bits
	// We need this first to sign the ca certificate / cert.pem
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return &certs, err
	}

	// certificate authority (ca) PEM encoded
	caBytes, err := generateCA(notValidBefore, notValidAfter, serialNumber, subject, priv)
	if err != nil {
		return &certs, err
	}
	certs.CA = caBytes

	// This is the server certificate
	serverCert, err := generateX509(
		notValidBefore,
		notValidAfter,
		serialNumber,
		subject,
		priv,
		false,
		host,
	)
	if err != nil {
		return &certs, err
	}

	// ...and the client certificate!
	clientCert, err := generateX509(
		notValidBefore,
		notValidAfter,
		serialNumber,
		subject,
		priv,
		true,
		host,
	)
	if err != nil {
		return &certs, err
	}
	certs.Certificate = serverCert
	certs.Client = clientCert

	// These are the permissions we will need
	// keyOut, err := os.OpenFile("key.pem", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	var keyOut bytes.Buffer
	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return &certs, err
	}
	err = pem.Encode(&keyOut, &pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})
	if err != nil {
		return &certs, err
	}
	certs.Key = keyOut.Bytes()

	// Generate a TLS server certificate keypair
	// Not sure if we need this, might as well make it
	pairCert, err := tls.X509KeyPair(certs.Certificate, certs.Key)
	if err != nil {
		return nil, err
	}
	certs.ServerCert = &pairCert
	return &certs, nil
}
