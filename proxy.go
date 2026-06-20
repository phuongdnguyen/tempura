package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"go.temporal.io/api/workflowservice/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/proto"
)

type TemporalProxy struct {
	Server   *http.Server
	Resolver *Resolver
	ConnPark *ConnPark
}

func NewTemporalProxy(listenAddr string, registry *VirtualNamespaceRegistry) (*TemporalProxy, error) {
	defaultTargetURLStr := "http://localhost:7233"
	defaultTarget, err := url.Parse(defaultTargetURLStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse default target URL: %w", err)
	}

	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			if req.URL.Host == "" {
				req.URL.Host = defaultTarget.Host
				req.Host = defaultTarget.Host
			}
		},
	}

	// Temporal uses gRPC, which requires HTTP/2. Since we aren't using TLS (h2c),
	// we need to configure the proxy's transport to allow HTTP/2 over cleartext TCP.
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}
	proxy.Transport = transport

	mappings := NewMappings("./mappings.json")
	resolver := NewResolver(mappings)

	connPark := NewConnPark(resolver, registry, transport)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if the RPC call is for StartWorkflowExecution
		if strings.Contains(r.URL.Path, "StartWorkflowExecution") {
			// Read the payload from the request body
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				// gRPC payload structure:
				// 1 byte compressed flag
				// 4 bytes message length
				// N bytes protobuf payload
				fmt.Printf("Intercepted StartWorkflowExecution!\n")
				fmt.Printf("Path: %s\n", r.URL.Path)
				fmt.Printf("Payload Length: %d bytes\n", len(bodyBytes))

				newBodyBytes, err := handleStartWorkflowExecution(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing StartWorkflowExecution: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}

				// Recreate the body so the proxy can still forward it to the upstream server
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			} else {
				log.Printf("Error reading request body: %v\n", err)
			}
		} else if strings.Contains(r.URL.Path, "PollWorkflowTaskQueue") {
			servePollWorkflowTaskQueue(w, r, connPark)
			return // already handled, do not fall through to ReverseProxy
		} else if strings.Contains(r.URL.Path, "PollActivityTaskQueue") {
			servePollActivityTaskQueue(w, r, connPark)
			return // already handled, do not fall through to ReverseProxy
		} else if strings.Contains(r.URL.Path, "RespondWorkflowTaskCompleted") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				newBodyBytes, err := handleRespondWorkflowTaskCompleted(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing RespondWorkflowTaskCompleted: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			} else {
				log.Printf("Error reading request body: %v\n", err)
			}
		} else if strings.Contains(r.URL.Path, "RespondActivityTaskCompleted") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				newBodyBytes, err := handleRespondActivityTaskCompleted(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing RespondActivityTaskCompleted: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			} else {
				log.Printf("Error reading request body: %v\n", err)
			}
		}

		// Forward the request to the target Temporal server
		proxy.ServeHTTP(w, r)
	})

	// We wrap our handler in h2c.NewHandler to allow the proxy server itself
	// to accept HTTP/2 connections over plaintext from the Temporal client.
	h2cHandler := h2c.NewHandler(handler, &http2.Server{})

	server := &http.Server{
		Addr:    listenAddr,
		Handler: h2cHandler,
	}

	return &TemporalProxy{
		Server:   server,
		Resolver: resolver,
		ConnPark: connPark,
	}, nil
}

// Start runs the proxy server and blocks until it stops or an error occurs.
func (tp *TemporalProxy) Start() error {
	log.Printf("Starting lightweight proxy on %s \n", tp.Server.Addr)
	return tp.Server.ListenAndServe()
}

// Stop gracefully shuts down the proxy server.
func (tp *TemporalProxy) Stop(ctx context.Context) error {
	log.Printf("Stopping proxy on %s\n", tp.Server.Addr)
	return tp.Server.Shutdown(ctx)
}

func handleStartWorkflowExecution(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	if len(bodyBytes) <= 5 {
		return nil, fmt.Errorf("payload too short to be valid gRPC")
	}

	var pbPayload []byte
	isCompressed := bodyBytes[0] == 1
	if isCompressed {
		gz, err := gzip.NewReader(bytes.NewReader(bodyBytes[5:]))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		pbPayload, err = io.ReadAll(gz)
		gz.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip payload: %w", err)
		}
	} else {
		pbPayload = bodyBytes[5:]
	}

	reqStruct := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(pbPayload, reqStruct); err != nil {
		return nil, fmt.Errorf("failed to unmarshal StartWorkflowExecutionRequest: %w", err)
	}

	fmt.Printf("Resolving workflowID: %s, namespaceID: %s\n", reqStruct.WorkflowId, reqStruct.Namespace)

	payload := &Payload{
		WorkflowID:       reqStruct.WorkflowId,
		VirtualNamespace: reqStruct.Namespace,
	}

	physNs, cacheHit := resolver.Resolve(payload, registry)
	fmt.Printf("Resolved virtual namespace '%s' to physical namespace '%s' (Cache hit: %v)\n", reqStruct.Namespace, physNs, cacheHit)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite the target namespace to the physical namespace we resolved
	reqStruct.Namespace = ns.name

	// Dynamically route this request to the resolved cluster
	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	// Re-marshal the updated protobuf payload
	newPbPayload, err := proto.Marshal(reqStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated StartWorkflowExecutionRequest: %w", err)
	}

	// We will send the payload uncompressed to avoid any gzip re-compression quirks.
	finalPayload := newPbPayload

	newBodyLen := len(finalPayload)
	newBody := make([]byte, 5+newBodyLen)
	// Set compression flag to 0 (uncompressed)
	newBody[0] = 0
	// Set the new message length in big-endian format
	newBody[1] = byte(newBodyLen >> 24)
	newBody[2] = byte(newBodyLen >> 16)
	newBody[3] = byte(newBodyLen >> 8)
	newBody[4] = byte(newBodyLen)

	copy(newBody[5:], finalPayload)

	// GRPc over HTTP/2 usually does not use Content-Length.
	// To be safe against length mismatch deadlocks, we delete it.
	r.ContentLength = -1
	r.Header.Del("Content-Length")

	return newBody, nil
}

func handleRespondWorkflowTaskCompleted(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	if len(bodyBytes) <= 5 {
		return nil, fmt.Errorf("payload too short to be valid gRPC")
	}

	var pbPayload []byte
	isCompressed := bodyBytes[0] == 1
	if isCompressed {
		gz, err := gzip.NewReader(bytes.NewReader(bodyBytes[5:]))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		pbPayload, err = io.ReadAll(gz)
		gz.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip payload: %w", err)
		}
	} else {
		pbPayload = bodyBytes[5:]
	}

	reqStruct := &workflowservice.RespondWorkflowTaskCompletedRequest{}
	if err := proto.Unmarshal(pbPayload, reqStruct); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RespondWorkflowTaskCompletedRequest: %w", err)
	}

	// Extract WorkflowID by parsing the TaskToken protobuf directly
	// rather than relying on reqStruct.ResourceId
	workflowID, err := extractWorkflowIDFromTaskToken(reqStruct.TaskToken)
	if err != nil || workflowID == "" {
		log.Printf("Warning: Failed to extract WorkflowId from TaskToken: %v", err)
		// Fallback to ResourceId just in case
		workflowID = reqStruct.ResourceId
	}

	fmt.Printf("Resolving RespondWorkflowTaskCompleted workflowID: '%s', namespaceID: '%s'\n", workflowID, reqStruct.Namespace)

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: reqStruct.Namespace,
	}

	physNs, cacheHit := resolver.Resolve(payload, registry)
	fmt.Printf("Resolved virtual namespace '%s' to physical namespace '%s' (Cache hit: %v)\n", reqStruct.Namespace, physNs, cacheHit)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite the target namespace to the physical namespace we resolved
	reqStruct.Namespace = ns.name

	// Dynamically route this request to the resolved cluster
	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	// Re-marshal the updated protobuf payload
	newPbPayload, err := proto.Marshal(reqStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated RespondWorkflowTaskCompletedRequest: %w", err)
	}

	finalPayload := newPbPayload

	newBodyLen := len(finalPayload)
	newBody := make([]byte, 5+newBodyLen)
	newBody[0] = 0 // uncompressed
	newBody[1] = byte(newBodyLen >> 24)
	newBody[2] = byte(newBodyLen >> 16)
	newBody[3] = byte(newBodyLen >> 8)
	newBody[4] = byte(newBodyLen)

	copy(newBody[5:], finalPayload)

	r.ContentLength = -1
	r.Header.Del("Content-Length")

	return newBody, nil
}

// extractWorkflowIDFromTaskToken parses the raw Temporal TaskToken protobuf bytes
// to extract the workflow_id, which is defined as field 2 (string) in the tokenspb.Task message.
func extractWorkflowIDFromTaskToken(token []byte) (string, error) {
	var i int
	for i < len(token) {
		// Read varint tag
		tag, n := binary.Uvarint(token[i:])
		if n <= 0 {
			return "", fmt.Errorf("invalid varint at index %d", i)
		}
		i += n

		fieldNum := tag >> 3
		wireType := tag & 7

		switch wireType {
		case 0: // Varint
			_, n = binary.Uvarint(token[i:])
			if n <= 0 {
				return "", fmt.Errorf("invalid varint value")
			}
			i += n
		case 1: // 64-bit
			i += 8
		case 2: // Length-delimited
			length, n := binary.Uvarint(token[i:])
			if n <= 0 {
				return "", fmt.Errorf("invalid length value")
			}
			i += n
			if fieldNum == 2 {
				// We found the workflow ID (field 2)
				if i+int(length) > len(token) {
					return "", fmt.Errorf("length-delimited field extends past end")
				}
				return string(token[i : i+int(length)]), nil
			}
			i += int(length)
		case 5: // 32-bit
			i += 4
		default:
			return "", fmt.Errorf("unsupported wire type %d", wireType)
		}
	}
	return "", fmt.Errorf("workflow_id (field 2) not found in TaskToken")
}

func servePollWorkflowTaskQueue(w http.ResponseWriter, r *http.Request, connPark *ConnPark) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v\n", err)
		http.Error(w, "Error reading request body", 500)
		return
	}

	resp, err := connPark.ExecutePoll(r, bodyBytes)
	if err != nil {
		log.Printf("ConnPark ExecutePoll failed: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	// Write the response back to the client
	for k, v := range resp.Header {
		w.Header()[k] = v
	}

	// Declare trailers before writing header (optional but good practice)
	if len(resp.Trailer) > 0 {
		var trailers []string
		for k := range resp.Trailer {
			trailers = append(trailers, k)
		}
		w.Header().Set("Trailer", strings.Join(trailers, ","))
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)

	// After body is fully read from cluster, write the trailers to client
	for k, v := range resp.Trailer {
		for _, vv := range v {
			w.Header().Add(http.TrailerPrefix+k, vv)
		}
	}
}

func handleRespondActivityTaskCompleted(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	if len(bodyBytes) <= 5 {
		return nil, fmt.Errorf("payload too short to be valid gRPC")
	}

	var pbPayload []byte
	isCompressed := bodyBytes[0] == 1
	if isCompressed {
		gz, err := gzip.NewReader(bytes.NewReader(bodyBytes[5:]))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		pbPayload, err = io.ReadAll(gz)
		gz.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip payload: %w", err)
		}
	} else {
		pbPayload = bodyBytes[5:]
	}

	reqStruct := &workflowservice.RespondActivityTaskCompletedRequest{}
	if err := proto.Unmarshal(pbPayload, reqStruct); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RespondActivityTaskCompletedRequest: %w", err)
	}

	// Extract WorkflowID directly from TaskToken
	workflowID, err := extractWorkflowIDFromTaskToken(reqStruct.TaskToken)
	if err != nil || workflowID == "" {
		log.Printf("Warning: Failed to extract WorkflowId from Activity TaskToken: %v", err)
		workflowID = reqStruct.ResourceId
	}

	fmt.Printf("Resolving RespondActivityTaskCompleted workflowID: '%s', namespaceID: '%s'\n", workflowID, reqStruct.Namespace)

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: reqStruct.Namespace,
	}

	physNs, cacheHit := resolver.Resolve(payload, registry)
	fmt.Printf("Resolved virtual namespace '%s' to physical namespace '%s' (Cache hit: %v)\n", reqStruct.Namespace, physNs, cacheHit)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite namespace
	reqStruct.Namespace = ns.name

	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	newPbPayload, err := proto.Marshal(reqStruct)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated RespondActivityTaskCompletedRequest: %w", err)
	}

	newBodyLen := len(newPbPayload)
	newBody := make([]byte, 5+newBodyLen)
	newBody[0] = 0 // uncompressed
	newBody[1] = byte(newBodyLen >> 24)
	newBody[2] = byte(newBodyLen >> 16)
	newBody[3] = byte(newBodyLen >> 8)
	newBody[4] = byte(newBodyLen)
	copy(newBody[5:], newPbPayload)

	r.ContentLength = -1
	r.Header.Del("Content-Length")

	return newBody, nil
}

func servePollActivityTaskQueue(w http.ResponseWriter, r *http.Request, connPark *ConnPark) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error reading request body: %v\n", err)
		http.Error(w, "Error reading request body", 500)
		return
	}

	resp, err := connPark.ExecutePollActivity(r, bodyBytes)
	if err != nil {
		log.Printf("ConnPark ExecutePollActivity failed: %v", err)
		http.Error(w, err.Error(), 500)
		return
	}

	for k, v := range resp.Header {
		w.Header()[k] = v
	}

	if len(resp.Trailer) > 0 {
		var trailers []string
		for k := range resp.Trailer {
			trailers = append(trailers, k)
		}
		w.Header().Set("Trailer", strings.Join(trailers, ","))
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)

	for k, v := range resp.Trailer {
		for _, vv := range v {
			w.Header().Add(http.TrailerPrefix+k, vv)
		}
	}
}
