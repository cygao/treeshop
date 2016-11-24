"""
Treehouse Workshop

Experimental Python Fabric file to spin up machines,
copy files to them, run a dockerized pipeline, and
copy the results back.

For Openstack you just set OS_USERNAME and OS_PASSWORD
environment variables to your Openstack username and
password.

For Azure you must run docker-machine first on the command
line to log in via URL
"""
import re
import csv
from fabric.api import env, local, run, runs_once

"""
Setup the fabric hosts environment using docker-machine
ip addresses and ssh keys. This is enable fabric run and sudo
to work as expected. An alternative would be to use the
docker-machine ssh alternative but that's not as pretty.
"""
env.user = "ubuntu"
env.hosts = re.findall(r'[0-9]+(?:\.[0-9]+){3}',
                       local("docker-machine ls --format '{{.URL}}'", capture=True))
env.key_filename = ["~/.docker/machine/machines/{}/id_rsa".format(m) for m in
                    local("docker-machine ls --format '{{.Name}}'", capture=True).split("\n")]


def hello():
    """ Run echo $HOSTNAME on each machine inside a docker container """
    run("docker run alpine /bin/echo ""Hello from $HOSTNAME""")


def create(count=1, flavor="m1.small"):
    """ Create 'count' (default=1) 'flavor' (default=m1.small) machines """
    for i in range(0, int(count)):
        local("""
            docker-machine create --driver openstack \
                --openstack-tenant-name treehouse \
                --openstack-auth-url http://os-con-01.pod:5000/v2.0 \
                --openstack-ssh-user ubuntu \
                --openstack-flavor-name {} \
                --openstack-net-name treehouse-net \
                --openstack-floatingip-pool ext-net \
                --openstack-image-name Ubuntu-16.04-LTS-x86_64 \
                treehouse-pipeline-{}
              """.format(flavor, i))
        # Add ubuntu to docker group so we can do run("docker...") vs. sudo
        local("docker-machine ssh {} sudo gpasswd -a ubuntu".format(i))


@runs_once
def ips():
    """ Print IPs of each machine """
    print env.hosts


def terminate():
    """ Terminate all machines """
    hosts = local("docker-machine ls --quiet", capture=True).strip()
    hosts = hosts.split("\n") if hosts else []
    for host in hosts:
        local("docker-machine stop -f {}".format(host))
        local("docker-machine rm -f {}".format(host))


def defuse(manifest):
    """ Run defuse on all fastq pairs in 'manifest' """
    print manifest
    with open(manifest) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            for file in row["samples"].split(","):
                local("scp {} {}".format(file, env.hosts[0]))
