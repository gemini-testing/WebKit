package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// ProxyServer represents the HTTP proxy server
type ProxyServer struct {
	backendURL   *url.URL
	requestQueue chan *ProxyRequest
	client       *http.Client
}

// ProxyRequest represents a queued request with response channel
type ProxyRequest struct {
	req        *http.Request
	respWriter http.ResponseWriter
	done       chan struct{}
}

// NewProxyServer creates a new proxy server instance
func NewProxyServer(backendURL string) (*ProxyServer, error) {
	parsedURL, err := url.Parse(backendURL)
	if err != nil {
		return nil, fmt.Errorf("invalid backend URL: %v", err)
	}

	// Create HTTP client with connection reuse and keep-alive
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			DisableKeepAlives:   false,
		},
	}

	proxy := &ProxyServer{
		backendURL:   parsedURL,
		requestQueue: make(chan *ProxyRequest, 1000), // Buffer for incoming requests
		client:       client,
	}

	return proxy, nil
}

// waitForBackend polls the backend server until it becomes accessible
func (p *ProxyServer) waitForBackend() error {
	log.Println("Waiting for backend server to become accessible...")

	timeout := 10 * time.Second
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeoutChan := time.After(timeout)

	// Create HTTP client with short timeout for health checks
	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("backend server did not become accessible within %v", timeout)
		case <-ticker.C:
			// Try to connect to backend with HEAD request
			req, err := http.NewRequest("HEAD", p.backendURL.String()+"/", nil)
			if err != nil {
				continue // Skip this tick and try again
			}

			resp, err := client.Do(req)
			if err != nil {
				continue // Backend not ready yet, try again
			}
			resp.Body.Close()

			// Backend responded with any HTTP status - it's accessible
			log.Printf("Backend server is now accessible - received HTTP %d", resp.StatusCode)
			return nil
		}
	}
}

// Start begins the proxy server operations
func (p *ProxyServer) Start() {
	// Start the request processor goroutine
	go p.processRequests()
}

// processRequests handles queued requests one at a time
func (p *ProxyServer) processRequests() {
	for proxyReq := range p.requestQueue {
		p.handleSingleRequest(proxyReq)
		close(proxyReq.done)
	}
}

// handleSingleRequest processes a single request to the backend
func (p *ProxyServer) handleSingleRequest(proxyReq *ProxyRequest) {
	req := proxyReq.req
	w := proxyReq.respWriter

	// Log received request
	log.Printf("Received request: %s %s", req.Method, req.URL.Path)

	// Read the request body once and create a copy for the backend request
	var bodyBytes []byte
	var err error
	if req.Body != nil {
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			log.Printf("Sent response: %s %s - %d (Error reading request body: %v)", req.Method, req.URL.Path, http.StatusInternalServerError, err)
			http.Error(w, fmt.Sprintf("Error reading request body: %v", err), http.StatusInternalServerError)
			return
		}
		req.Body.Close()

		// Log request body
		if len(bodyBytes) > 0 {
			bodyStr := string(bodyBytes)
			if len(bodyStr) > 1000 {
				bodyStr = bodyStr[:1000] + "..."
			}
			log.Printf("Data: %s", bodyStr)
		}
	}

	// Create new request to backend with copied body
	var bodyReader io.Reader
	if len(bodyBytes) > 0 {
		bodyReader = bytes.NewReader(bodyBytes)
	}

	backendURL := p.backendURL.String() + req.URL.Path
	if req.URL.RawQuery != "" {
		backendURL += "?" + req.URL.RawQuery
	}

	backendReq, err := http.NewRequest(req.Method, backendURL, bodyReader)
	if err != nil {
		log.Printf("Sent response: %s %s - %d (Error creating backend request: %v)", req.Method, req.URL.Path, http.StatusInternalServerError, err)
		http.Error(w, fmt.Sprintf("Error creating backend request: %v", err), http.StatusInternalServerError)
		return
	}

	// Copy all headers from original request
	for name, values := range req.Header {
		// Skip hop-by-hop headers that shouldn't be forwarded
		if name == "Connection" || name == "Proxy-Connection" ||
			name == "Te" || name == "Trailer" || name == "Upgrade" {
			continue
		}
		for _, value := range values {
			backendReq.Header.Add(name, value)
		}
	}

	// Override Host header as specified
	backendReq.Header.Set("Host", p.backendURL.Host)
	backendReq.Host = p.backendURL.Host

	// Ensure Content-Length is set correctly if we have a body
	if len(bodyBytes) > 0 {
		backendReq.ContentLength = int64(len(bodyBytes))
	}

	// Send request to backend
	resp, err := p.client.Do(backendReq)
	if err != nil {
		log.Printf("Sent response: %s %s - %d (Error contacting backend: %v)", req.Method, req.URL.Path, http.StatusBadGateway, err)
		http.Error(w, fmt.Sprintf("Error contacting backend: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Log received response from backend
	log.Printf("Received response: %s %s - %d", req.Method, req.URL.Path, resp.StatusCode)

	// Read response body for logging and forwarding
	respBodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %v", err)
		http.Error(w, "Error reading response body", http.StatusInternalServerError)
		return
	}
	resp.Body.Close()

	// Log response body
	respBodyStr := string(respBodyBytes)
	if len(respBodyStr) > 1000 {
		respBodyStr = respBodyStr[:1000] + "..."
	}
	log.Printf("Data: %s", respBodyStr)

	// Copy response headers (skip hop-by-hop headers)
	for name, values := range resp.Header {
		if name == "Connection" || name == "Proxy-Connection" ||
			name == "Te" || name == "Trailer" || name == "Upgrade" {
			continue
		}
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}

	// Set response status code
	w.WriteHeader(resp.StatusCode)

	// Write response body from bytes
	_, err = w.Write(respBodyBytes)
	if err != nil {
		log.Printf("Error writing response body: %v", err)
	}
}

// ServeHTTP implements the http.Handler interface
func (p *ProxyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Create a proxy request
	proxyReq := &ProxyRequest{
		req:        r,
		respWriter: w,
		done:       make(chan struct{}),
	}

	// Queue the request
	select {
	case p.requestQueue <- proxyReq:
		// Wait for the request to be processed
		<-proxyReq.done
	default:
		// Queue is full
		log.Printf("Sent response: %s %s - %d (Proxy queue is full)", r.Method, r.URL.Path, http.StatusServiceUnavailable)
		http.Error(w, "Proxy queue is full", http.StatusServiceUnavailable)
	}
}

// Shutdown gracefully shuts down the proxy server
func (p *ProxyServer) Shutdown() {
	close(p.requestQueue)
}

func main() {
	// Create proxy server instance
	proxy, err := NewProxyServer("http://127.0.0.1:4445")
	if err != nil {
		log.Fatalf("Failed to create proxy server: %v", err)
	}

	// Wait for backend server to become accessible
	if err := proxy.waitForBackend(); err != nil {
		log.Fatalf("Failed to start proxy: %v", err)
	}

	// Start the proxy server
	proxy.Start()

	// Set up HTTP server
	server := &http.Server{
		Addr:    ":4444",
		Handler: proxy,
	}

	log.Println("Starting HTTP proxy server on port 4444...")
	log.Println("Proxying requests to 127.0.0.1:4445")

	// Start listening
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed to start: %v", err)
	}
}
