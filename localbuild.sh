kubectl delete -f deployment.yaml
docker build --rm -t demo .
docker tag demo asia.gcr.io/mobingi-main/hedge:$1
docker push asia.gcr.io/mobingi-main/hedge:$1
docker rmi $(docker images --filter "dangling=true" -q --no-trunc) -f
sed -i -e 's/image\:\ asia.gcr.io\/mobingi\-main\/hedge[\:@].*$/image\:\ asia.gcr.io\/mobingi\-main\/hedge\:'$1'/g' deployment.yaml
