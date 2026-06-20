package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"go.temporal.io/sdk/client"
)

func main() {
	// Connect to the proxy running on localhost:8088
	c, err := client.Dial(client.Options{
		HostPort:  "localhost:8088",
		Namespace: "default",
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client:", err)
	}
	defer c.Close()

	numWorkflows := 100
	var wg sync.WaitGroup
	wg.Add(numWorkflows)

	fmt.Printf("Starting %d workflows via proxy (localhost:8088)...\n", numWorkflows)
	start := time.Now()

	// Use a semaphore to limit concurrency so we don't exhaust resources immediately
	sem := make(chan struct{}, 100)

	for i := 0; i < numWorkflows; i++ {
		sem <- struct{}{} // acquire token
		go func(index int) {
			defer wg.Done()
			defer func() { <-sem }() // release token

			workflowID := fmt.Sprintf("test-workflow-%d-%d", time.Now().UnixNano(), index)
			workflowOptions := client.StartWorkflowOptions{
				ID:        workflowID,
				TaskQueue: "test-task-queue",
			}

			// We are just starting the workflow; the Temporal server will accept it
			// even if there is no worker listening to "test-task-queue".
			// Note: We use ExecuteWorkflow to send a StartWorkflowExecution request.
			_, err := c.ExecuteWorkflow(context.Background(), workflowOptions, "DummyWorkflowType")
			if err != nil {
				log.Printf("Failed to start workflow %d: %v", index, err)
			}
		}(i)
	}

	wg.Wait()
	fmt.Printf("Finished starting %d workflows in %v.\n", numWorkflows, time.Since(start))
}
