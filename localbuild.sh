kubectl delete -f deployment.yaml
DOCKER_BUILDKIT=0 docker build --rm -t demo .
DOCKER_BUILDKIT=0 docker tag demo asia.gcr.io/mobingi-main/hedge:$1
DOCKER_BUILDKIT=0 docker push asia.gcr.io/mobingi-main/hedge:$1
DOCKER_BUILDKIT=0 docker rmi $(docker images --filter "dangling=true" -q --no-trunc) -f
[ -f deployment-private.yaml ] && sed -i -e 's/image\:\ asia.gcr.io\/mobingi\-main\/hedge[\:@].*$/image\:\ asia.gcr.io\/mobingi\-main\/hedge\:'$1'/g' deployment-private.yaml
