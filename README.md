# GoSearch

A distributed web crawler and indexer running on Kubernetes. The crawler discovers and fetches pages, publishing them to Kafka. The indexer consumes from Kafka and stores structured content in Elasticsearch.

## Architecture

- **Crawler** (Go) — 3-replica Deployment; consumes hosts from `crawler-hosts`, publishes pages to `crawler-pages`
- **Init Job** (Go) — one-shot Kubernetes Job that seeds the initial host into `crawler-hosts`
- **Indexer** (Python) — consumes `crawler-pages`, parses HTML, and writes to Elasticsearch
- **Kafka** (Strimzi) — two topics: `crawler-hosts` (6 partitions), `crawler-pages` (6 partitions)
- **Redis** — distributed host-claim deduplication (SETNX with TTL)
- **Elasticsearch** (ECK) — document store for indexed pages

---

## Prerequisites

- Docker Desktop with Kubernetes enabled
- `kubectl` and `helm` CLIs
- Local image registry running on port 5000

Start the local registry if not already running:
```
docker run -d -p 5000:5000 --name registry registry:2
```

---

## Deployment Order

Deploy infrastructure first (Kafka, Redis, Elasticsearch), then applications (Indexer, Crawler, Init Job).

---

## 1. Kafka

```bash
kubectl create namespace kafka
helm repo add strimzi https://strimzi.io/charts/
helm repo update
helm install strimzi strimzi/strimzi-kafka-operator -n kafka
```

Wait for the operator pod to be Running, then apply the cluster and topics:

```bash
kubectl apply -f charts/kafka/kafka-cluster.yaml
kubectl apply -f charts/kafka/kafka-nodepool.yaml
kubectl apply -f charts/kafka/crawler-topic.yaml
kubectl apply -f charts/kafka/crawler-hosts-topic.yaml
```

Verify:
```bash
kubectl get kafka -n kafka          # Ready column should be True
kubectl get pods -n kafka           # kafka-cluster-dual-role-0, entity-operator, strimzi-cluster-operator all Running
kubectl get svc -n kafka            # kafka-cluster-kafka-bootstrap should be present
```

---

## 2. Redis

```bash
kubectl apply -f charts/redis/redis.yaml
```

Verify:
```bash
kubectl get pods -n redis           # redis pod Running
```

---

## 3. Elasticsearch

```bash
helm repo add elastic https://helm.elastic.co
helm repo update
helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace
kubectl create namespace elasticsearch
kubectl apply -f charts/elasticsearch/elasticsearch.yaml
```

Retrieve the generated password (needed for Indexer setup):
```bash
kubectl get secret elastic-es-elastic-user -n elasticsearch -o jsonpath='{.data.elastic}' | base64 -d
```

Verify:
```bash
kubectl get elasticsearch -n elasticsearch    # health should be green or yellow
kubectl get pods -n elasticsearch
```

---

## 4. Indexer

Create the config, credentials secret, then deploy:

```bash
kubectl create configmap indexer-config --from-file=./internal/indexer/indexer_config.yaml

kubectl create secret generic indexer-es-credentials \
  --from-literal=username=elastic \
  --from-literal=password=<password>      # from step 3
```

Build and push the image:
```bash
docker build -t localhost:5000/indexer:latest -f ./internal/indexer/Dockerfile .
docker push localhost:5000/indexer:latest
```

Deploy:
```bash
helm install indexer ./charts/indexer
```

Upgrade after code changes:
```bash
helm upgrade indexer ./charts/indexer
```

---

## 5. Crawler

Create a `cmd/crawler/config.yaml` (gitignored — do not commit):
```yaml
kafka:
  brokers:
    - kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
  pagesTopic: crawler-pages
  hostsTopic: crawler-hosts

redis:
  addr: redis.redis.svc.cluster.local:6379
  claimTTL: 24h

crawler:
  maxWorkers: 5
  maxPagesPerHost: 500
  maxBodyBytes: 524288    # 512KB

filter:
  blockedKeywords: []
  blockedDomains: []
```

Create the ConfigMap, build and push, then deploy:
```bash
kubectl create namespace crawler
kubectl create configmap crawler-config --from-file=cmd/crawler/config.yaml -n crawler

docker build -t localhost:5000/crawler:latest -f ./cmd/crawler/Dockerfile .
docker push localhost:5000/crawler:latest

helm install crawler ./charts/crawler -n crawler
```

Upgrade after code changes:
```bash
docker build -t localhost:5000/crawler:latest -f ./cmd/crawler/Dockerfile .
docker push localhost:5000/crawler:latest
kubectl rollout restart deployment/crawler -n crawler
```

---

## 6. Init Job (Seed URL)

Create a `cmd/init-job/config.yaml` (gitignored — do not commit):
```yaml
kafka:
  brokers:
    - kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092
  hostsTopic: crawler-hosts

seed: https://www.example.com
```

Create the ConfigMap, build and push, then run the job:
```bash
kubectl create configmap init-job-config --from-file=cmd/init-job/config.yaml -n crawler

docker build -t localhost:5000/init-job:latest -f ./cmd/init-job/Dockerfile .
docker push localhost:5000/init-job:latest

helm install init-job ./charts/init-job -n crawler
```

To re-seed (e.g. after flushing Kafka or Redis), delete and reinstall:
```bash
helm uninstall init-job -n crawler
helm install init-job ./charts/init-job -n crawler
```

---

## Teardown

```bash
helm uninstall crawler -n crawler
helm uninstall init-job -n crawler
helm uninstall indexer

kubectl delete configmap crawler-config -n crawler
kubectl delete configmap init-job-config -n crawler
kubectl delete configmap indexer-config
kubectl delete secret indexer-es-credentials

kubectl delete -f charts/redis/redis.yaml
kubectl delete -f charts/kafka/crawler-hosts-topic.yaml
kubectl delete -f charts/kafka/crawler-topic.yaml
kubectl delete -f charts/kafka/kafka-nodepool.yaml
kubectl delete -f charts/kafka/kafka-cluster.yaml
helm uninstall strimzi -n kafka
```

---

## Useful Commands

```bash
# Crawler logs (all pods)
kubectl logs -n crawler -l app=crawler --tail=50

# Document count in Elasticsearch
ES_PASS=$(kubectl get secret elastic-es-elastic-user -n elasticsearch -o jsonpath='{.data.elastic}' | base64 -d)
kubectl exec -n elasticsearch elastic-es-default-0 -- \
  curl -s -u "elastic:$ES_PASS" http://localhost:9200/pages/_count

# Flush Redis (reset host claims)
kubectl exec -n redis deploy/redis -- redis-cli FLUSHALL

# Check Kafka consumer group lag
kubectl exec -n kafka kafka-cluster-dual-role-0 -- \
  bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group crawler-group
```
