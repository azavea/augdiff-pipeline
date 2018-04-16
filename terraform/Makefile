

ADI_REPO ?= $(shell terraform output | grep adi_container_address | awk '{print $$NF}')
AUGDIFF_REPO ?= $(shell terraform output | grep augdiff_container_address | awk '{print $$NF}')

build_adi:
	docker build -f job-definitions/adi-update.dockerfile -t ${ADI_REPO} ./job-definitions/

push_adi:
	docker push ${ADI_REPO}:latest

build_augdiff:
	docker build -f job-definitions/augdiff.dockerfile -t ${AUGDIFF_REPO} ./job-definitions/

push_augdiff:
	docker push ${AUGDIFF_REPO}:latest

docker_login:
	aws ecr get-login --region us-east-1


