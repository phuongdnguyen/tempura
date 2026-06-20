package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

// DummyActivityType is a simple activity executed by the workflow
func DummyActivityType(ctx context.Context, name string) (string, error) {
	fmt.Printf("DummyActivityType started for: %s\n", name)
	time.Sleep(1 * time.Second) // Simulate some work
	return fmt.Sprintf("Hello %s from Activity!", name), nil
}

// DummyWorkflowType is the simple workflow executed by start_workflows.go
func DummyWorkflowType(ctx workflow.Context) (string, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("DummyWorkflowType started and completed via proxy!")

	ao := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
	}
	ctx = workflow.WithActivityOptions(ctx, ao)

	logger.Info("Executing DummyActivityType...")
	var result string
	err := workflow.ExecuteActivity(ctx, DummyActivityType, "Temporal").Get(ctx, &result)
	if err != nil {
		logger.Error("Activity failed", "Error", err)
		return "", err
	}

	logger.Info("Activity completed successfully!", "Result", result)
	return "Success: " + result, nil
}

func main() {
	// Connect to the Temporal Proxy
	c, err := client.Dial(client.Options{
		HostPort:  "localhost:8088",
		Namespace: "default",
	})
	if err != nil {
		log.Fatalln("Unable to create Temporal client:", err)
	}
	defer c.Close()

	// Create a new worker that listens to the same task queue used in start_workflows.go
	w := worker.New(c, "test-task-queue", worker.Options{})

	// Register our dummy workflow and activity
	w.RegisterWorkflow(DummyWorkflowType)
	w.RegisterActivity(DummyActivityType)

	fmt.Println("Starting Temporal Worker...")
	fmt.Println("Polling proxy at localhost:8088 on namespace 'default' and task queue 'test-task-queue'")
	
	// Start the worker and block until interrupted
	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Worker failed to start:", err)
	}
}
