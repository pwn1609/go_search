# GoSearch

*Deployment steps*
If no registry running: docker run -d -p 5000:5000 --name registry registry:2

Crawler:
- docker build -t localhost:5000/crawler:latest -f ./cmd/crawler/Dockerfile .
- docker push localhost:5000/crawler:latest
- helm install crawler ./charts/crawler

- uninstall - helm uninstall crawler

Indexer:
- kubectl create configmap indexer-config --from-file=./internal/indexer/indexer_config.yaml
 - kubectl delete configmap indexer-config

 - kubectl create secret generic indexer-es-credentials --from-literal=username=elastic --from-literal=password=<password>
  - Get password: kubectl get secret elastic-es-elastic-user -n elasticsearch -o jsonpath='{.data.elastic}' | base64 -d
  - kubectl delete secret indexer-es-credentials

- docker build -t localhost:5000/indexer:latest -f ./internal/indexer/Dockerfile .
- docker push localhost:5000/indexer:latest

helm install indexer ./charts/indexer
 - helm upgrade indexer ./charts/indexer (if already installed)


kafka:
- kubectl create namespace kafka
- helm repo add strimzi https://strimzi.io/charts/
- helm repo update
- helm install strimzi strimzi/strimzi-kafka-operator -n kafka
cd ../charts/kafka
- kubectl apply -f kafka-cluster.yaml
- kubectl apply -f kafka-nodepool.yaml
- kubectl apply -f crawler-topic.yaml

Verify Kafka:
kubectl describe kafka kafka-cluster -n kafka - Should see "strimzi-cluster-operator-xxxx   Running"
kubectl get kafka -n kafka - Should see "kafka-cluster    True"
 - If not: kubectl describe kafka kafka-cluster -n kafka
kubectl get pods -n kafka - Should see:
 - kafka-cluster-dual-role-0      Running
 - kafka-cluster-entity-operator  Running
 - strimzi-cluster-operator       Running
kubectl get svc -n kafka - Should see: 
 - kafka-cluster-kafka-bootstrap
 - kafka-cluster-kafka-brokers

ElasticSearch:
helm repo add elastic https://helm.elastic.co
helm repo update
helm install elastic-operator elastic/eck-operator -n elastic-system --create-namespace
kubectl create namespace elasticsearch
kubectl apply -f ./charts/elasticsearch/elasticsearch.yaml
 - Note: ECK manages security automatically. Retrieve the elastic user password after deployment:
   kubectl get secret elastic-es-elastic-user -n elasticsearch -o jsonpath='{.data.elastic}' | base64 -d

Verify ElasticSearch:
kubectl get elasticsearch -n elasticsearch
kubectl get pods -n elasticsearch
kubectl get svc -n elasticsearch
kubectl get pvc -n elasticsearch


Indexer should create the index
curl -X PUT http://quickstart-es-http:9200/web-pages \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": {
      "properties": {
        "url":        { "type": "keyword" },
        "domain":     { "type": "keyword" },
        "title":      { "type": "text" },
        "headings": { "type": "text" },
        "content":    { "type": "text" },
        "timestamp":  { "type": "date" },
        "status_code":{ "type": "integer" }
      }
    }
  }'
