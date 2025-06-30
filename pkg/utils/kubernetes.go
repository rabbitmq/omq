package utils

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/rabbitmq/omq/pkg/log"
)

// K8sGet performs a GET request on the Kubernetes API with the given URL;
// We only need 1 GET on a simple REST endpoint, and this way we avoid
// importing go-client with lots of dependencies
func GetEndpoints(serviceName string) ([]string, error) {
	const (
		tokenFile  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
		rootCAFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	)

	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	namespace := os.Getenv("MY_POD_NAMESPACE")
	url := fmt.Sprintf("https://%s:%s/api/v1/namespaces/%s/endpoints/%s", host, port, namespace, serviceName)

	log.Info("Getting endpoints from Kubernetes", "url", url)

	token, err := os.ReadFile(tokenFile)
	if err != nil {
		log.Error("Can't read the Kubernetes token", "error", err.Error())
		os.Exit(1)
	}
	bearer := "Bearer " + string(token)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Error("Can't create a request to the Kubernetes API", "error", err.Error())
		os.Exit(1)
	}

	req.Header.Add("Authorization", bearer)

	certPool := x509.NewCertPool()
	caCert, err := os.ReadFile(rootCAFile)
	if err != nil {
		log.Error("Can't read the Kubernetes CA certificate", "error", err.Error())
		os.Exit(1)
	}
	certPool.AppendCertsFromPEM(caCert)
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs: certPool,
		},
	}

	client := &http.Client{Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		log.Error("Can't connect to the Kubernetes API", "error", err.Error())
		os.Exit(1)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	responseData, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Error("", "error", err)
	}

	if resp.StatusCode != http.StatusOK {
		return []string{}, fmt.Errorf("kubernetes API returned an error: %s", resp.Status)
	}
	return parseEndpoints(responseData)
}

func parseEndpoints(data []byte) ([]string, error) {
	type Endpoints struct {
		Subsets []struct {
			Addresses []struct {
				IP string `json:"ip"`
			} `json:"addresses"`
		} `json:"subsets"`
	}

	var endpoints Endpoints
	if err := json.Unmarshal(data, &endpoints); err != nil {
		return []string{}, err
	}

	var ips []string
	if len(endpoints.Subsets) > 0 {
		for _, address := range endpoints.Subsets[0].Addresses {
			ips = append(ips, address.IP)
		}
	}
	return ips, nil
}
