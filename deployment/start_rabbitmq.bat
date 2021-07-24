pushd "%~dp0"
kubectl apply -f .\rabbitmq_deployment.yml
popd
@REM CALL minikube service rabbitmq