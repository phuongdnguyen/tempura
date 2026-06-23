# Tempura

Tempura is a semantic aware load balancer for Temporal clusters with the goal to achieve active-active mode to unlock higher scaling capability.

# Usage
Start dependencies, assuming you are on mac
```bash
docker-compose -f sandbox/docker-compose.yml up -d
```
Run tempura
```bash
go build 
./tempura
```

Start a worker
```bash
go run scripts/worker.go
```

Start some workflows
```bash
go run scripts/start_workflows.go
```

# Architecture
Sticky proxy
![Architecture](./docs/images/arc.png)
Write-back task polling
![Polling](./docs/images/polling.png)

# License
Apache 2.0