# TedX

TedX is a semantic aware load balancer for Temporal clusters with the goal to achieve active-active mode to unlock higher scaling capability.

> [!CAUTION]
> This is a POC and not ready for production use. It is intended to be used as a reference implementation for building a production-ready solution.

# Usage
Start dependencies, assuming you are on mac
```bash
docker-compose -f sandbox/docker-compose.yml up -d
```
Run tedx
```bash
go build 
./tedx
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