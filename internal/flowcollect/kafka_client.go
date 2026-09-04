package flowcollect

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"github.com/Shopify/sarama"
)

func buildKafkaClientConfig(config KafkaConfig, clientID string) (*sarama.Config, error) {
	if clientID == "" {
		return nil, errors.New("Kafka client ID is required")
	}
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = clientID
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Metadata.AllowAutoTopicCreation = false
	saramaConfig.Net.TLS.Enable = config.TLS
	if config.TLS {
		tlsConfig, err := buildKafkaTLSConfig(config)
		if err != nil {
			return nil, err
		}
		saramaConfig.Net.TLS.Config = tlsConfig
	}
	return saramaConfig, nil
}

func buildKafkaTLSConfig(config KafkaConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.TLSServerName}
	if config.TLSCAFile != "" {
		pem, err := readBoundedFile(config.TLSCAFile, 4<<20)
		if err != nil {
			return nil, fmt.Errorf("read Kafka TLS CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("Kafka TLS CA file contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	if config.TLSCertFile != "" || config.TLSKeyFile != "" {
		if config.TLSCertFile == "" || config.TLSKeyFile == "" {
			return nil, errors.New("Kafka TLS certificate and key must be configured together")
		}
		certPEM, err := readBoundedFile(config.TLSCertFile, 4<<20)
		if err != nil {
			return nil, fmt.Errorf("read Kafka TLS client certificate: %w", err)
		}
		keyPEM, err := readBoundedFile(config.TLSKeyFile, 4<<20)
		if err != nil {
			return nil, fmt.Errorf("read Kafka TLS client key: %w", err)
		}
		certificate, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("load Kafka TLS client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return tlsConfig, nil
}
