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
from fabric.api import env, local, run, runs_once, parallel
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


@parallel
def defuse_references():
    # run("sudo wget -P /mnt/references http://ceph-gw-01.pod/references/defuse_index.tar.gz")
    local("docker-machine scp"
          "/pod/pstore/users/jpfeil/references/defuse_index.tar.gz {}:/mnt/references"
          .format(env.host))


def defuse(manifest):
    """ Run defuse on all the samples in 'manifest' """
    with open(manifest) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            for sample in row["File Path"].split(","):
                dest = "/mnt/inputs/{}".format(os.path.basename(sample))
                if exists(dest):
                    print "Skipping, already exists".format(dest)
                else:
                    print "Copying {} to {}".format(sample, dest)
                    local("docker-machine scp {} {}:{}".format(sample, env.hostnames[0], dest))
            run("""
                docker run -it --rm \
                    -v /mnt/inputs:/data \
                    -v /mnt/outputs:/outputs \
                    -v /mnt/references/defuse_index:/data/defuse_index \
                    jpfeil/defuse-pipeline:latest \
                    -1 {} -2 {} \
                    -o /outputs \
                    -n {}
                """.format(os.path.basename(row["File Path"].split(",")[0]),
                           os.path.basename(row["File Path"].split(",")[1]),
                           row["Submitter Sample ID"]))
            return


def defuse_one():
    run("""
        docker run -it --rm \
            -v /mnt/inputs:/data \
            -v /mnt/outputs:/outputs \
            -v /mnt/references/defuse_index:/data/defuse_index \
            jpfeil/defuse-pipeline:latest \
            -1 SRR1988322_1.fastq.gz -2 SRR1988322_2.fastq.gz \
            -o /outputs \
            -n SRR1988322
        """)
