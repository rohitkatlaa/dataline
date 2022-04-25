# cd ../pipeline_creation
# docker build -t rohitkatlaa/dataline_pipeline_creation:latest .
# cd ../pipeline_execution
# docker build -t rohitkatlaa/dataline_pipeline_execution:latest .
# docker push rohitkatlaa/dataline_pipeline_creation:latest
# docker push rohitkatlaa/dataline_pipeline_execution:latest
# cd ../k8-configuration
kubectl apply -f dataline-namespace.yaml
kubectl apply -f postgres-secret.yaml --namespace=dataline
kubectl apply -f postgres.yaml --namespace=dataline
kubectl apply -f postgres-service.yaml --namespace=dataline
kubectl apply -f postgres-configmap.yaml --namespace=dataline
kubectl apply -f pipeline_creation.yaml --namespace=dataline
kubectl apply -f pipeline_creation_service.yaml --namespace=dataline
kubectl apply -f pipeline_execution.yaml --namespace=dataline
kubectl apply -f pipeline_execution_service.yaml --namespace=dataline
kubectl apply -f dataline_ingress.yaml --namespace=dataline