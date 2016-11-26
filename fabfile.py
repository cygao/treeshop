"""
Treeshop: The Treehouse Workshop

Experimental Python Fabric file to spin up machines,
copy files to them, run a dockerized pipeline, and
copy the results back.

For Openstack you just set OS_USERNAME and OS_PASSWORD
environment variables to your Openstack username and
password.

For Azure you must run docker-machine first on the command
line to log in via URL
"""
import os
import re
import datetime
import csv
from fabric.api import env, local, run, runs_once, parallel, warn_only
from fabric.contrib.files import exists

"""
Setup the fabric hosts environment using docker-machine
ip addresses and ssh keys. This enables fabric run and sudo
to work as expected. An alternative would be to use the
docker-machine ssh alternative but that's not as pretty.
Note we use ips in 'hosts' as the machine names are
not resolvable.
"""
env.user = "ubuntu"
env.hostnames = local("docker-machine ls --format '{{.Name}}'", capture=True).split("\n")
env.hosts = re.findall(r'[0-9]+(?:\.[0-9]+){3}',
                       local("docker-machine ls --format '{{.URL}}'", capture=True))
env.key_filename = ["~/.docker/machine/machines/{}/id_rsa".format(m) for m in env.hostnames]


@runs_once
def machines():
    """ Print hostname and ips of each machine """
    print "Hostnames:", env.hostnames
    print "IPs:", env.hosts
    print "SSH Keys:", env.key_filename


def create(count=1, flavor="m1.small"):
    """ Create 'count' (default=1) 'flavor' (default=m1.small) machines """
    for i in range(0, int(count)):
        name = "{}-treeshop-{}".format(os.environ["USER"],
                                       datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
        local("""
            docker-machine create --driver openstack \
                --openstack-tenant-name treehouse \
                --openstack-auth-url http://os-con-01.pod:5000/v2.0 \
                --openstack-ssh-user ubuntu \
                --openstack-flavor-name {} \
                --openstack-net-name treehouse-net \
                --openstack-floatingip-pool ext-net \
                --openstack-image-name Ubuntu-16.04-LTS-x86_64 \
                {}
              """.format(flavor, name))
        # Add ubuntu to docker group so we can do run("docker...") vs. sudo
        local("docker-machine ssh {} sudo gpasswd -a ubuntu docker".format(name))


def terminate():
    """ Terminate all machines """
    for host in env.hostnames:
        local("docker-machine stop -f {}".format(host))
        local("docker-machine rm -f {}".format(host))


@parallel
def hello():
    """ Run echo $HOSTNAME on each machine inside a docker container """
    print "Running against", env.host
    run("docker run alpine /bin/echo ""Hello from $HOSTNAME""")


@parallel
def configure():
    """ Configure folders on each machine """
    run("sudo mkdir -p /mnt/inputs")
    run("sudo chown ubuntu:ubuntu /mnt/inputs")
    run("sudo mkdir -p /mnt/outputs")
    run("sudo chown ubuntu:ubuntu /mnt/outputs")
    run("sudo mkdir -p /mnt/references")
    run("sudo chown ubuntu:ubuntu /mnt/references")


def defuse_references():
    # wget won't work yet on azure
    # run("sudo wget -P /mnt/references http://ceph-gw-01.pod/references/defuse_index.tar.gz")
    if not exists("/mnt/inputs/defuse_index"):
        print "Copying defuse references"
        local("docker-machine scp"
              "/pod/pstore/users/jpfeil/references/defuse_index.tar.gz {}:/mnt/inputs"
              .format(env.host))
        print "Untarring defuse references"
        run("tar -xvf /mnt/inputs/defuse_index.tar.gz")
    else:
        print "Defuse references already installed"


@parallel
def defuse(manifest):
    """ Run defuse on all the samples in 'manifest' """
    samples = list(csv.DictReader(open(manifest), delimiter="\t"))

    # Split manifest up among all hosts for poor mans round robin task allocation
    for i in range(env.hosts.index(env.host), len(samples), len(env.hostnames)):
        print "Running defuse on {} sample: {}".format(i, samples[i]["Submitter Sample ID"])
        fastqs = samples[i]["File Path"].split(",")
        for fastq in fastqs:
            dest = "/mnt/inputs/{}".format(os.path.basename(fastq))
            if exists(os.path.splitext(dest)[0]):
                print "Skipping, {} already exists".format(dest)
            else:
                print "Copying {} to {}".format(fastq, dest)
                local("docker-machine scp {} {}:{}".format(fastq, env.hostnames[0], dest))
                run("gunzip {}".format(dest))
        with warn_only():
            run("docker stop defuse && docker rm defuse")
        run("""
            docker run -it --rm --name defuse \
                -v /mnt/inputs:/data \
                -v /mnt/outputs:/outputs \
                jpfeil/defuse-pipeline:latest \
                -1 {} -2 {} \
                -o /outputs \
                -p `nproc` \
                -n {}
            """.format(os.path.basename(os.path.splitext(fastqs[0])[0]),
                       os.path.basename(os.path.splitext(fastqs[1])[0]),
                       samples[i]["Submitter Sample ID"]))
        # For now just do one sample
        return
