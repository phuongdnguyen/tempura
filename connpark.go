package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/proto"
)

type ConnPark struct {
	resolver  *Resolver
	registry  *VirtualNamespaceRegistry
	transport http.RoundTripper
}

func NewConnPark(resolver *Resolver, registry *VirtualNamespaceRegistry, transport http.RoundTripper) *ConnPark {
	return &ConnPark{
		resolver:  resolver,
		registry:  registry,
		transport: transport,
	}
}

type pollResult struct {
	resp   *http.Response
	physNs string
	err    error
}

func (cp *ConnPark) ExecutePoll(originalReq *http.Request, bodyBytes []byte) (*http.Response, error) {
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

	reqStruct := &workflowservice.PollWorkflowTaskQueueRequest{}
	if err := proto.Unmarshal(pbPayload, reqStruct); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PollWorkflowTaskQueueRequest: %w", err)
	}

	virtualNamespaceStr := reqStruct.Namespace
	virtualNs := cp.registry.Resolve(virtualNamespaceStr)
	if virtualNs == nil {
		return nil, fmt.Errorf("virtual namespace not found: %s", virtualNamespaceStr)
	}

	slots := virtualNs.Hasher.GetAllSlots()
	if len(slots) == 0 {
		return nil, fmt.Errorf("no physical namespaces found for virtual namespace: %s", virtualNamespaceStr)
	}

	log.Printf("Fanning out PollWorkflowTaskQueue to %d clusters for virtual namespace %s", len(slots), virtualNamespaceStr)

	ctx, cancel := context.WithCancel(originalReq.Context())
	defer cancel()

	resultCh := make(chan pollResult, len(slots))
	var wg sync.WaitGroup

	for _, physNs := range slots {
		wg.Add(1)
		go func(targetPhysNs string) {
			defer wg.Done()
			res, err := cp.pollSingleCluster(ctx, originalReq, reqStruct, targetPhysNs, isCompressed)

			// We only want to send successful tasks or capture the final error
			// If it's a timeout (empty task token), it's considered successful but we shouldn't immediately return it
			// unless all clusters timed out.
			// Let's send everything to the channel and filter.
			resultCh <- pollResult{resp: res, physNs: targetPhysNs, err: err}
		}(physNs)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var lastEmptyResp *http.Response
	var lastErr error

	for res := range resultCh {
		if res.err != nil {
			lastErr = res.err
			continue
		}

		// Read the response body to check for TaskToken
		respBodyBytes, err := io.ReadAll(res.resp.Body)
		res.resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if len(respBodyBytes) > 5 && res.resp.StatusCode == 200 {
			respPbPayload := respBodyBytes[5:]
			respIsCompressed := respBodyBytes[0] == 1
			if respIsCompressed {
				gz, err := gzip.NewReader(bytes.NewReader(respPbPayload))
				if err == nil {
					respPbPayload, _ = io.ReadAll(gz)
					gz.Close()
				}
			}

			pollResp := &workflowservice.PollWorkflowTaskQueueResponse{}
			if err := proto.Unmarshal(respPbPayload, pollResp); err == nil {
				if len(pollResp.TaskToken) > 0 {
					// WE FOUND A TASK!
					log.Printf("Received task from %s!", res.physNs)

					// Cache the WorkflowID -> Physical Namespace routing
					if pollResp.WorkflowExecution != nil && pollResp.WorkflowExecution.WorkflowId != "" {
						workflowID := pollResp.WorkflowExecution.WorkflowId
						cp.resolver.Cache.Put(workflowID, res.physNs)
						log.Printf("Cached WorkflowID %s -> %s", workflowID, res.physNs)
					}

					// Reconstruct response body so it can be forwarded
					res.resp.Body = io.NopCloser(bytes.NewBuffer(respBodyBytes))

					// Cancel other pending requests
					cancel()
					return res.resp, nil
				}
			}
		}

		// If it has no TaskToken (timeout) or invalid, keep it as fallback
		lastEmptyResp = res.resp
		lastEmptyResp.Body = io.NopCloser(bytes.NewBuffer(respBodyBytes))
	}

	// If we get here, no cluster returned a task.
	if lastEmptyResp != nil {
		log.Printf("All clusters timed out. Returning empty response.")
		return lastEmptyResp, nil
	}

	return nil, fmt.Errorf("all polling attempts failed, last error: %v", lastErr)
}

func (cp *ConnPark) pollSingleCluster(ctx context.Context, originalReq *http.Request, reqStruct *workflowservice.PollWorkflowTaskQueueRequest, physNs string, wasCompressed bool) (*http.Response, error) {
	// Deep copy the struct to avoid data races
	clonedReqStruct := proto.Clone(reqStruct).(*workflowservice.PollWorkflowTaskQueueRequest)

	ns := parsePhysicalNamespace(physNs)
	clonedReqStruct.Namespace = ns.name

	newPbPayload, err := proto.Marshal(clonedReqStruct)
	if err != nil {
		return nil, err
	}

	newBodyLen := len(newPbPayload)
	newBody := make([]byte, 5+newBodyLen)
	newBody[0] = 0 // send uncompressed
	newBody[1] = byte(newBodyLen >> 24)
	newBody[2] = byte(newBodyLen >> 16)
	newBody[3] = byte(newBodyLen >> 8)
	newBody[4] = byte(newBodyLen)
	copy(newBody[5:], newPbPayload)

	req := originalReq.Clone(ctx)
	req.URL.Scheme = "http"
	req.URL.Host = ns.cluster.address
	req.Host = ns.cluster.address
	req.ContentLength = -1
	req.Header.Del("Content-Length")
	req.Body = io.NopCloser(bytes.NewBuffer(newBody))
	req.RequestURI = "" // Must be empty for client requests

	return cp.transport.RoundTrip(req)
}

func (cp *ConnPark) ExecutePollActivity(originalReq *http.Request, bodyBytes []byte) (*http.Response, error) {
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

	reqStruct := &workflowservice.PollActivityTaskQueueRequest{}
	if err := proto.Unmarshal(pbPayload, reqStruct); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PollActivityTaskQueueRequest: %w", err)
	}

	virtualNamespaceStr := reqStruct.Namespace
	virtualNs := cp.registry.Resolve(virtualNamespaceStr)
	if virtualNs == nil {
		return nil, fmt.Errorf("virtual namespace not found: %s", virtualNamespaceStr)
	}

	slots := virtualNs.Hasher.GetAllSlots()
	if len(slots) == 0 {
		return nil, fmt.Errorf("no physical namespaces found for virtual namespace: %s", virtualNamespaceStr)
	}

	log.Printf("[ConnPark] Fanning out PollActivityTaskQueue to %d clusters for virtual namespace %s", len(slots), virtualNamespaceStr)

	ctx, cancel := context.WithCancel(originalReq.Context())
	defer cancel()

	resultCh := make(chan pollResult, len(slots))
	var wg sync.WaitGroup

	for _, physNs := range slots {
		wg.Add(1)
		go func(targetPhysNs string) {
			defer wg.Done()
			res, err := cp.pollSingleActivityCluster(ctx, originalReq, reqStruct, targetPhysNs, isCompressed)
			resultCh <- pollResult{resp: res, physNs: targetPhysNs, err: err}
		}(physNs)
	}

	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var lastEmptyResp *http.Response
	var lastErr error

	for res := range resultCh {
		if res.err != nil {
			lastErr = res.err
			continue
		}

		respBodyBytes, err := io.ReadAll(res.resp.Body)
		res.resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}

		if len(respBodyBytes) > 5 && res.resp.StatusCode == 200 {
			respPbPayload := respBodyBytes[5:]
			respIsCompressed := respBodyBytes[0] == 1
			if respIsCompressed {
				gz, err := gzip.NewReader(bytes.NewReader(respPbPayload))
				if err == nil {
					respPbPayload, _ = io.ReadAll(gz)
					gz.Close()
				}
			}

			pollResp := &workflowservice.PollActivityTaskQueueResponse{}
			if err := proto.Unmarshal(respPbPayload, pollResp); err == nil {
				if len(pollResp.TaskToken) > 0 {
					log.Printf("[ConnPark] Received activity task from %s!", res.physNs)
					
					if pollResp.WorkflowExecution != nil && pollResp.WorkflowExecution.WorkflowId != "" {
						workflowID := pollResp.WorkflowExecution.WorkflowId
						cp.resolver.Cache.Put(workflowID, res.physNs)
						log.Printf("[ConnPark] Cached WorkflowID %s -> %s (from Activity Poll)", workflowID, res.physNs)
					}

					res.resp.Body = io.NopCloser(bytes.NewBuffer(respBodyBytes))
					cancel()
					return res.resp, nil
				}
			}
		}

		lastEmptyResp = res.resp
		lastEmptyResp.Body = io.NopCloser(bytes.NewBuffer(respBodyBytes))
	}

	if lastEmptyResp != nil {
		log.Printf("[ConnPark] All clusters timed out polling for activity. Returning empty response.")
		return lastEmptyResp, nil
	}

	return nil, fmt.Errorf("all polling attempts failed, last error: %v", lastErr)
}

func (cp *ConnPark) pollSingleActivityCluster(ctx context.Context, originalReq *http.Request, reqStruct *workflowservice.PollActivityTaskQueueRequest, physNs string, wasCompressed bool) (*http.Response, error) {
	clonedReqStruct := proto.Clone(reqStruct).(*workflowservice.PollActivityTaskQueueRequest)

	ns := parsePhysicalNamespace(physNs)
	clonedReqStruct.Namespace = ns.name

	newPbPayload, err := proto.Marshal(clonedReqStruct)
	if err != nil {
		return nil, err
	}

	newBodyLen := len(newPbPayload)
	newBody := make([]byte, 5+newBodyLen)
	newBody[0] = 0 // send uncompressed
	newBody[1] = byte(newBodyLen >> 24)
	newBody[2] = byte(newBodyLen >> 16)
	newBody[3] = byte(newBodyLen >> 8)
	newBody[4] = byte(newBodyLen)
	copy(newBody[5:], newPbPayload)

	req := originalReq.Clone(ctx)
	req.URL.Scheme = "http"
	req.URL.Host = ns.cluster.address
	req.Host = ns.cluster.address
	req.ContentLength = -1
	req.Header.Del("Content-Length")
	req.Body = io.NopCloser(bytes.NewBuffer(newBody))
	req.RequestURI = ""

	return cp.transport.RoundTrip(req)
}
