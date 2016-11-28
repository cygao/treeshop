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
from fabric.api import env, local, run, sudo, runs_once, parallel, warn_only, cd
from fabric.contrib.files import exists
from fabric.operations import put, get
from fabric.utils import abort

"""
Setup the fabric hosts environment using docker-machine ip addresses as hostnames are not
resolvable. Also point to all the per machine ssh keys. An alternative would be to use one key but
on openstack the driver deletes it on termination.
"""
env.user = "ubuntu"
env.hostnames = local("docker-machine ls --format '{{.Name}}'", capture=True).split("\n")
env.hosts = re.findall(r'[0-9]+(?:\.[0-9]+){3}',
                       local("docker-machine ls --format '{{.URL}}'", capture=True))
env.key_filename = ["~/.docker/machine/machines/{}/id_rsa".format(m) for m in env.hostnames]


@runs_once
def machines():
    """ Print hostname, ip, and ssh key location of each machine """
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
    """ Run echo $HOSTNAME in parallel in a container on each machine. """
    print "Running against", env.host
    run("docker run alpine /bin/echo ""Hello from $HOSTNAME""")


def configure(verify="True"):
    """ Configure each machine with reference files. """
    # Put everything in data as on openstack you can't chown /mnt
    sudo("mkdir -p /mnt/data")
    sudo("chown ubuntu:ubuntu /mnt/data")
    run("mkdir -p /mnt/data/references")
    run("mkdir -p /mnt/data/samples")
    run("mkdir -p /mnt/data/outputs")
    with cd("/mnt/data/references"):
        if not exists("STARFusion-GRCh38gencode23"):
            put("/pod/pstore/users/jpfeil/references/STARFusion-GRCh38gencode23.tar.gz")
            run("tar -xvf STARFusion-GRCh38gencode23.tar.gz")
        for r in ["kallisto_hg38.idx",
                  "starIndex_hg38_no_alt.tar.gz", "rsem_ref_hg38_no_alt.tar.gz"]:
            run("wget -nv -N http://hgdownload.soe.ucsc.edu/treehouse/reference/{}".format(r))
        if verify == "True":
            put("rnaseq.md5")
            run("md5sum -c rnaseq.md5")
            put("defuse.md5")
            run("md5sum -c defuse.md5")


def _run_rnaseq(r1, r2, name):
    run("tar -cf samples/{}.tar samples/{}.gz samples/{}.gz".format(name, r1, r2))
    run("""
        docker run -it --rm --name rnaseq \
            -v /mnt/data/outputs:/mnt/data/outputs \
            -v /mnt/data/samples:/samples \
            -v /mnt/data/references:/references \
            -v /var/run/docker.sock:/var/run/docker.sock \
            quay.io/ucsc_cgl/rnaseq-cgl-pipeline:2.0.8 \
            --save-bam \
            --star /references/starIndex_hg38_no_alt.tar.gz \
            --rsem /references/rsem_ref_hg38_no_alt.tar.gz \
            --kallisto /references/kallisto_hg38.idx \
            --samples /samples/{}.tar
        """.format(name))


def _run_defuse(r1, r2, name):
    run("""
        docker run -it --rm --name defuse \
            -v /mnt/data:/data \
            jpfeil/star-fusion:0.0.1 \
            --CPU `nproc` \
            --genome_lib_dir references/STARFusion-GRCh38gencode23 \
            --left_fq samples/{} --right_fq samples/{} --output_dir outputs/{}
        """.format(r1, r2, name))


def _clean():
    # Stop an existing processing
    with warn_only():
        run("docker stop rnaseq && docker rm rnaseq")
        run("docker stop defuse && docker rm defuse")
        run("docker stop qc && docker rm qc")
    sudo("rm -rf /mnt/data/samples/*")
    sudo("rm -rf /mnt/data/outputs/*")


@parallel
def process(manifest, outputs="/pod/pstore/groups/treehouse/treeshop/outputs",
            rnaseq="True", qc="True", defuse="True"):
    """ Run defuse on all the samples in 'manifest' """
    samples = list(csv.DictReader(open(manifest), delimiter="\t"))

    # Split manifest up among all hosts for poor mans round robin task allocation
    for i in range(env.hosts.index(env.host), len(samples), len(env.hosts)):
        _clean()
        with cd("/mnt/data"):
            sample = samples[i]

            # Copy fastqs
            if (rnaseq == "True") or (defuse == "True"):
                fastqs = sample["File Path"].split(",")
                for fastq in fastqs:
                    if not exists("samples/{}".format(os.path.basename(fastq))):
                        put(fastq, "samples/{}".format(os.path.basename(fastq)))
                        run("gunzip -k samples/{}".format(os.path.basename(fastq)))

            # Copy bam as if it came from the output of RNASeq
            if (qc == "True") and (rnaseq != "True"):  # qc only so copy bam
                if not sample["File Path"].endwith(".bam"):
                    abort("Expected bam for row {} {}".format(i, sample["Submitter Sample ID"]))
                put(sample["File Path"], "outputs/")

            # Run each pipeline
            if rnaseq == "True":
                _run_rnaseq(os.path.basename(os.path.splitext(fastqs[0])[0]),
                            os.path.basename(os.path.splitext(fastqs[1])[0]),
                            sample["Submitter Sample ID"])
                get("outputs/{}.tar.gz", outputs)

            if defuse == "True":
                _run_defuse(os.path.basename(os.path.splitext(fastqs[0])[0]),
                            os.path.basename(os.path.splitext(fastqs[1])[0]),
                            sample["Submitter Sample ID"])
                get("outputs/star-fusion.fusion_candidates.final.whitelist.abridged",
                    "{}/{}.defuse.whitelist.abridged".format(
                        outputs, sample["Submitter Sample ID"]))
                get("outputs/star-fusion.fusion_candidates.final.final.abridged",
                    "{}/{}.defuse.final.abridged".format(
                        outputs, sample["Submitter Sample ID"]))


def verify():
    # Verify md5 of rnaseq output from TEST samples
    put("TEST_RNA.md5", "/mnt/outputs")
    run("tar -xOzvf /mnt/outputs/TEST.tar.gz "
        "TEST/RSEM/rsem.genes.norm_counts.tab | "
        "md5sum -c /mnt/outputs/TEST.md5")

# @parallel
# def qc(manifest):
#     """ QC on all the bams in the manifest """
#     samples = list(csv.DictReader(open(manifest), delimiter="\t"))

#     # Split manifest up among all hosts for poor mans round robin task allocation
#     for i in range(env.hosts.index(env.host), len(samples), len(env.hosts)):
#         print "Running qc on {} sample: {}".format(i, samples[i]["Submitter Sample ID"])
#         dest = "/mnt/inputs/{}".format(os.path.basename(fastq))
#         if exists(os.path.splitext(dest)[0]):
#             print "Skipping, {} already exists".format(dest)
#         else:
#             print "Copying {} to {}".format(fastq, dest)
#             local("docker-machine scp {} {}:{}".format(fastq, env.hostnames[0], dest))
#             run("gunzip {}".format(dest))
#         with warn_only():
#             run("docker stop defuse && docker rm defuse")
#         run("""
#             docker run -it --rm --name defuse \
#                 -v /mnt/inputs:/data \
#                 -v /mnt/outputs:/outputs \
#                 jpfeil/defuse-pipeline:gmap-latest \
#                 -1 {} -2 {} \
#                 -o /outputs \
#                 -p `nproc` \
#                 -n {}
#             """.format(os.path.basename(os.path.splitext(fastqs[0])[0]),
#                        os.path.basename(os.path.splitext(fastqs[1])[0]),
#                        samples[i]["Submitter Sample ID"]))
#         # For now just do one sample
#         return
