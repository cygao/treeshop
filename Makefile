ts := `/bin/date "+%Y%m%d-%H%M%S"`

create-openstack:
	docker-machine create --driver openstack \
		--openstack-tenant-name treehouse \
		--openstack-auth-url http://os-con-01.pod:5000/v2.0 \
		--openstack-flavor-name m1.small \
		--openstack-ssh-user ubuntu \
		--openstack-net-name treehouse-net \
		--openstack-floatingip-pool ext-net \
		--openstack-image-name Ubuntu-16.04-LTS-x86_64 \
		$(USER)-treeshop-$(ts)
	docker-machine ssh $(USER)-treeshop-$(ts) sudo gpasswd -a ubuntu docker

create-azure:
	# Sizes: https://docs.microsoft.com/en-us/azure/virtual-machines/virtual-machines-linux-sizes
	docker-machine create --driver azure \
		--azure-subscription-id 11ef7f2c-6e06-44dc-a389-1d6b1bea9489 \
		--azure-resource-group treeshop \
		--azure-username ubuntu \
		--azure-image canonical:UbuntuServer:16.04.0-LTS:latest \
		--azure-size Standard_D1 \
		$(USER)-treeshop-$(ts)
	docker-machine ssh $(USER)-treeshop-$(ts) sudo gpasswd -a ubuntu docker

terminate:
	-docker-machine stop $(USER)-treeshop-0
	-docker-machine rm -f $(USER)-treeshop-0

