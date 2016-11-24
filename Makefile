create:
	docker-machine create --driver openstack \
		--openstack-tenant-name treehouse \
		--openstack-auth-url http://os-con-01.pod:5000/v2.0 \
		--openstack-flavor-name m1.small \
		--openstack-ssh-user ubuntu \
		--openstack-net-name treehouse-net \
		--openstack-floatingip-pool ext-net \
		--openstack-image-name Ubuntu-16.04-LTS-x86_64 \
		treehouse-factory-0

terminate:
	docker-machine stop treehouse-factory-0
	docker-machine rm -f treehouse-factory-0
