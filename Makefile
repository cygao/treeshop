ts := $(shell /bin/date "+%Y%m%d-%H%M%S")

AZURE_FLAVOR=Standard_D1
OS_FLAVOR=m1.small

create-openstack:
	# Start an openstack docker-machine, specify size by make create-openstack OS_FLAVOR=m1.small
	docker-machine create --driver openstack \
		--openstack-tenant-name treehouse \
		--openstack-auth-url http://os-con-01.pod:5000/v2.0 \
		--openstack-ssh-user ubuntu \
		--openstack-net-name treehouse-net \
		--openstack-floatingip-pool ext-net \
		--openstack-image-name Ubuntu-16.04-LTS-x86_64 \
		--openstack-flavor-name $(OS_FLAVOR) \
		$(USER)-treeshop-$(ts)
	docker-machine ssh $(USER)-treeshop-$(ts) sudo gpasswd -a ubuntu docker

create-azure:
	# Start an azure docker-machine, specify size by make create-azure AZURE_FLAVOR=Standard_D1
	# Sizes: https://docs.microsoft.com/en-us/azure/virtual-machines/virtual-machines-linux-sizes
	docker-machine create --driver azure \
		--azure-subscription-id 11ef7f2c-6e06-44dc-a389-1d6b1bea9489 \
		--azure-resource-group treeshop \
		--azure-ssh-user ubuntu \
		--azure-image canonical:UbuntuServer:16.04.0-LTS:latest \
		--azure-size $(AZURE_FLAVOR) \
		$(USER)-treeshop-$(ts)
	docker-machine ssh $(USER)-treeshop-$(ts) sudo gpasswd -a ubuntu docker

terminate:
	# Stop and remove ALL docker-machines in ALL environments
	-docker-machine stop `docker-machine ls --format '{{.Name}}'`
	-docker-machine rm -f `docker-machine ls --format '{{.Name}}'`
