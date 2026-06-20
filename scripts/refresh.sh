#!/usr/bin/env bash
temporal operator namespace delete --address=localhost:7234 default-2 --yes
temporal operator namespace create --address=localhost:7234 default-2
temporal operator namespace delete --address=localhost:7233 default-1  --yes
temporal operator namespace create --address=localhost:7233 default-1