package main

import (
	"bytes"
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

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/proto"
)

type StickyProxy struct {
	Server   *http.Server
	Resolver *Resolver
	ConnPark *ConnPark
}

func NewStickyProxy(listenAddr string, redisAddr string, defaultTargetURLStr string, registry *VirtualNamespaceRegistry) (*StickyProxy, error) {
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

	mappings := NewMappings(redisAddr)
	resolver := NewResolver(mappings)

	connPark := NewConnPark(resolver, registry, transport)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if the RPC call is for StartWorkflowExecution
		if strings.Contains(r.URL.Path, "StartWorkflowExecution") {
			// Read the payload from the request body
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
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
		} else if strings.Contains(r.URL.Path, "CancelWorkflowExecution") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				newBodyBytes, err := handleCancelWorkflow(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing CanceWorkflow: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		} else if strings.Contains(r.URL.Path, "GetWorkflowExecutionHistory") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				newBodyBytes, err := handleGetWorkflowExecutionHistory(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing GetWorkflowExecutionHistory: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		} else if strings.Contains(r.URL.Path, "SignalWorkflowExecution") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				newBodyBytes, err := handleSignalWorkflow(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing SignalWorkflow: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		} else if strings.Contains(r.URL.Path, "QueryWorkflow") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				newBodyBytes, err := handleQueryWorkflow(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing QueryWorkflow: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			}
		} else if strings.Contains(r.URL.Path, "RespondQueryTaskCompleted") {
			bodyBytes, err := io.ReadAll(r.Body)
			if err == nil {
				newBodyBytes, err := handleRespondQueryTaskCompleted(r, bodyBytes, resolver, registry)
				if err != nil {
					log.Printf("Error processing RespondQueryTaskCompleted: %v\n", err)
				} else {
					bodyBytes = newBodyBytes
				}
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
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

	return &StickyProxy{
		Server:   server,
		Resolver: resolver,
		ConnPark: connPark,
	}, nil
}

// Start runs the proxy server and blocks until it stops or an error occurs.
func (tp *StickyProxy) Start() error {
	log.Printf("Starting proxy on %s \n", tp.Server.Addr)
	return tp.Server.ListenAndServe()
}

// Stop gracefully shuts down the proxy server.
func (tp *StickyProxy) Stop(ctx context.Context) error {
	log.Printf("Stopping proxy on %s\n", tp.Server.Addr)
	return tp.Server.Shutdown(ctx)
}

func handleStartWorkflowExecution(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &workflowservice.StartWorkflowExecutionRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal StartWorkflowExecutionRequest: %w", err)
	}

	payload := &Payload{
		WorkflowID:       req.WorkflowId,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite the target namespace to the physical namespace we resolved
	req.Namespace = ns.name

	// Dynamically route this request to the resolved cluster
	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	// Re-marshal the updated protobuf payload
	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated StartWorkflowExecutionRequest: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
}

func handleRespondWorkflowTaskCompleted(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &workflowservice.RespondWorkflowTaskCompletedRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RespondWorkflowTaskCompletedRequest: %w", err)
	}

	// Extract WorkflowID by parsing the TaskToken
	workflowID, err := extractWorkflowIDFromTaskToken(req.TaskToken)
	if err != nil || workflowID == "" {
		log.Printf("Warning: Failed to extract WorkflowId from TaskToken: %v", err)
		// Fallback to ResourceId just in case
		workflowID = req.ResourceId
	}

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	// Delete from cache AFTER resolving,
	for _, command := range req.Commands {
		if command.CommandType == enums.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION {
			log.Printf("completing workflow: %s, removing from routing cache\n", workflowID)
			resolver.Cache.Delete(workflowID)
		}
	}

	ns := parsePhysicalNamespace(physNs)

	// Rewrite the target namespace to the physical namespace we resolved
	req.Namespace = ns.name

	// Dynamically route this request to the resolved cluster
	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	// Re-marshal the updated protobuf payload
	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated RespondWorkflowTaskCompletedRequest: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
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
		http.Error(w, "Error reading request body", 500)
		return
	}

	resp, err := connPark.ExecutePoll(r, bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	writeProxyResponse(w, resp)
}

func handleRespondActivityTaskCompleted(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &workflowservice.RespondActivityTaskCompletedRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RespondActivityTaskCompletedRequest: %w", err)
	}

	// Extract WorkflowID directly from TaskToken
	workflowID, err := extractWorkflowIDFromTaskToken(req.TaskToken)
	if err != nil || workflowID == "" {
		log.Printf("Warning: Failed to extract WorkflowId from Activity TaskToken: %v", err)
		workflowID = req.ResourceId
	}

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite namespace
	req.Namespace = ns.name

	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated RespondActivityTaskCompletedRequest: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
}

func handleCancelWorkflow(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &workflowservice.RequestCancelWorkflowExecutionRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RespondActivityTaskCompletedRequest: %w", err)
	}
	workflowID := req.WorkflowExecution.GetWorkflowId()

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite namespace
	req.Namespace = ns.name

	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated RespondActivityTaskCompletedRequest: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
}

func handleSignalWorkflow(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &workflowservice.SignalWorkflowExecutionRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal SignalWorkflowExecutionRequest: %w", err)
	}
	fmt.Printf("Received Signal Workflow Request %+v\n", req)
	workflowID := req.WorkflowExecution.GetWorkflowId()

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite namespace
	req.Namespace = ns.name

	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated SignalWorkflowExecutionRequest: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
}

func handleQueryWorkflow(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &workflowservice.QueryWorkflowRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal QueryWorkflowRequest: %w", err)
	}
	workflowID := req.GetExecution().GetWorkflowId()

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite namespace
	req.Namespace = ns.name

	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated QueryWorkflowRequest: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
}

func handleRespondQueryTaskCompleted(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}
	req := &workflowservice.RespondQueryTaskCompletedRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal RespondQueryTaskCompletedRequest: %w", err)
	}
	workflowID, err := extractWorkflowIDFromTaskToken(req.TaskToken)
	if err != nil || workflowID == "" {
		log.Printf("Warning: Failed to extract WorkflowId from TaskToken: %v", err)
	}

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite namespace
	req.Namespace = ns.name

	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated RespondQueryTaskCompletedReqeust: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
}

func handleGetWorkflowExecutionHistory(r *http.Request, bodyBytes []byte, resolver *Resolver, registry *VirtualNamespaceRegistry) ([]byte, error) {
	pbPayload, _, err := extractPayload(bodyBytes)
	if err != nil {
		return nil, err
	}

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{}
	if err := proto.Unmarshal(pbPayload, req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal GetWorkflowExecutionHistoryRequest: %w", err)
	}
	workflowID := req.GetExecution().GetWorkflowId()

	payload := &Payload{
		WorkflowID:       workflowID,
		VirtualNamespace: req.Namespace,
	}

	physNs, _ := resolver.Resolve(payload, registry)

	ns := parsePhysicalNamespace(physNs)

	// Rewrite namespace
	req.Namespace = ns.name

	r.URL.Host = ns.cluster.address
	r.Host = ns.cluster.address

	newPbPayload, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal updated GetWorkflowExecutionHistoryRequest: %w", err)
	}

	return encodeGRPCPayload(newPbPayload, r), nil
}

func servePollActivityTaskQueue(w http.ResponseWriter, r *http.Request, connPark *ConnPark) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", 500)
		return
	}

	resp, err := connPark.ExecutePollActivity(r, bodyBytes)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}

	writeProxyResponse(w, resp)
}

func encodeGRPCPayload(finalPayload []byte, r *http.Request) []byte {
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

	return newBody
}

func writeProxyResponse(w http.ResponseWriter, resp *http.Response) {
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
