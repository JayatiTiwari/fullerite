package util

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// HTTPGet retrieve content from the given http/https URL
// Returns the response body, `Content-Type` header, and an error
func HTTPGet(
	url string,
	headers map[string]string,
	timeout int,
	serverCaFile string,
	clientCertFile string,
	clientKeyFile string,
) ([]byte, string, error) {

	var transport *http.Transport
	if clientCertFile != "" && clientKeyFile != "" && serverCaFile != "" {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, "", errors.Wrap(err, "Cannot load client credentials")
		}

		caCert, err := ioutil.ReadFile(serverCaFile)
		if err != nil {
			return nil, "", errors.Wrap(err, "Cannot load server CA")
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caCertPool,
		}
		tlsConfig.BuildNameToCertificate()
		transport = &http.Transport{TLSClientConfig: tlsConfig}
	} else {
		transport = &http.Transport{}
	}
	client := &http.Client{
		Timeout:   time.Duration(timeout) * time.Second,
		Transport: transport,
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", errors.Errorf("Error while creating a request for %s: %s", url, err)
	}
	for headerName, headerValue := range headers {
		req.Header.Add(headerName, headerValue)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", errors.Wrap(err, "Error while executing the request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", errors.Errorf("Server returned HTTP status %s", resp.Status)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, "", errors.Wrap(err, "Error while reading the response body")
	}

	return body, resp.Header.Get("Content-Type"), nil
}
