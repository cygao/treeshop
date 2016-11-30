"""
Treeshop: The Treehouse Workshop

Experimental fabric based automation to process
a manifest and run pipelines via docker-machine.
"""
import os
import re
import datetime
import csv
import json
import itertools
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


@parallel
def hello():
    """ Run echo $HOSTNAME in parallel in a container on each machine. """
    print "Running against", env.host
    run("docker run alpine /bin/echo ""Hello from $HOSTNAME""")


def configure(verify="True"):
    """ Configure each machine with reference files. """
    # Put everything in data as on openstack you can't chown /mnt
    run("sudo mkdir -p /mnt/data")
    run("sudo chown ubuntu:ubuntu /mnt/data")
    run("mkdir -p /mnt/data/references")
    run("mkdir -p /mnt/data/samples")
    run("mkdir -p /mnt/data/outputs")
    with cd("/mnt/data/references"):
        for ref in ["kallisto_hg38.idx",
                    "starIndex_hg38_no_alt.tar.gz",
                    "rsem_ref_hg38_no_alt.tar.gz",
                    "STARFusion-GRCh38gencode23.tar.gz"]:
            if not exists(ref):
                run("wget -nv -N https://treeshop.blob.core.windows.net/references/{}".format(ref))
        if not exists("STARFusion-GRCh38gencode23"):
            run("tar -xvf STARFusion-GRCh38gencode23.tar.gz")
        if verify == "True":
            put("refs.rnaseq.md5", "/mnt/data/references")
            run("md5sum -c refs.rnaseq.md5")
            put("refs.fusion.md5", "/mnt/data/references")
            run("md5sum -c refs.fusion.md5")


def _run_rnaseq(r1, r2, name):
    # RNASeq expects the fastqs tarred up...
    run("tar -cf samples/{}.tar samples/{} samples/{}".format(name, r1, r2))
    run("""
        docker run --rm --name rnaseq \
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
    # Generates name.tar.gz so untar and put in rnaseq folder
    run("tar -xzf outputs/{}.tar.gz -C outputs".format(name))
    run("mv outputs/{} outputs/rnaseq".format(name))
    return "quay.io/ucsc_cgl/rnaseq-cgl-pipeline:2.0.8"


def _run_qc(bam):
    run("mkdir outputs/qc")
    run("""
        docker run --rm --name qc \
            -v /mnt/data/outputs/qc:/data \
            -v {}:/data/rnaAligned.sortedByCoord.out.bam \
            hbeale/treehouse_bam_qc:1.0 runQC.sh
        """.format(bam))
    return "hbeale/treehouse_bam_qc:1.0"


def _run_fusion(r1, r2):
    run("mkdir outputs/fusion")
    run("""
        docker run --rm --name fusion \
            -v /mnt/data:/data \
            jpfeil/star-fusion:0.0.1 \
            --CPU `nproc` \
            --genome_lib_dir references/STARFusion-GRCh38gencode23 \
            --left_fq samples/{} --right_fq samples/{} --output_dir outputs/fusion
        """.format(r1, r2))
    return "jpfeil/star-fusion:0.0.1"


def _reset_machine():
    # Stop an existing processing and delete inputs and outputs
    with warn_only():
        run("docker stop rnaseq && docker rm rnaseq")
        run("docker stop fusion && docker rm fusion")
        run("docker stop qc && docker rm qc")
    sudo("rm -rf /mnt/data/samples/*")
    sudo("rm -rf /mnt/data/outputs/*")


@parallel
def process(manifest, outputs="/pod/pstore/groups/treehouse/treeshop/outputs",
            rnaseq="True", qc="True", fusion="True", limit=None):
    """ Process on all the samples in 'manifest' """

    # Each machine will process every #hosts samples
    for sample in itertools.islice(csv.DictReader(open(manifest), delimiter="\t"),
                                   env.hosts.index(env.host), limit, len(env.hosts)):
        sample_id = sample["Submitter Sample ID"]
        sample_files = sample["File Path"].split(",")
        print "{} processing {}".format(env.host, sample_id)

        _reset_machine()

        methods = {"user": os.environ["USER"],
                   "start": datetime.datetime.utcnow().isoformat(),
                   "treeshop_version": local("git describe --always", capture=True),
                   "inputs": sample_files,
                   "pipelines": []}

        with cd("/mnt/data"):
            # Copy fastqs
            if (rnaseq == "True") or (fusion == "True"):
                if len(sample_files) != 2:
                    abort("Expected 2 samples files")

                for fastq in sample_files:
                    if not exists("samples/{}".format(os.path.basename(fastq))):
                        put(fastq, "samples/{}".format(os.path.basename(fastq)))
                r1, r2 = [os.path.basename(f) for f in sample_files]

            # If only running qc then copy bam as if it came from rnaseq
            if (qc == "True") and (rnaseq != "True"):  # qc only so copy bam
                if not sample_files[0].endwith(".bam"):
                    abort("Expected bam for {}".format(sample_id))
                put(sample_files[0],
                    "outputs/{}.sorted.bam".format(sample_id))

            # Create folder on storage for results named after sample id
            results = "{}/{}".format(outputs, sample_id)
            local("mkdir -p {}".format(results))

            # rnaseq
            if rnaseq == "True":
                methods["pipelines"].append(_run_rnaseq(r1, r2, sample_id))
                get("outputs/rnaseq", results)

            # qc on rnaseq output bam
            if qc == "True" or rnaseq == "True":  # qc ALWAYS if rnaseq
                methods["pipelines"].append(
                    _run_qc("/mnt/data/outputs/{}.sorted.bam".format(sample_id)))
                get("outputs/qc", results)

            # fusion
            if fusion == "True":
                methods["pipelines"].append(_run_fusion(r1, r2))
                get("outputs/fusion", results)
                # get("outputs/star-fusion.fusion_candidates.final.whitelist.abridged",
                #     "{}/{}.genelistonly.fusion".format(
                #         outputs, sample_id))
                # get("outputs/star-fusion.fusion_candidates.final.final.abridged",
                #     "{}/{}.fusion".format(
                #         outputs, sample_id))

        # Write out methods
        methods["end"] = datetime.datetime.utcnow().isoformat()
        with open("{}/methods.json".format(results), "w") as f:
            f.write(json.dumps(methods, indent=4))


def verify():
    # Verify md5 of rnaseq output from TEST samples
    with cd("/mnt/data/outputs"):
        put("TEST.md5", "/mnt/data/outputs")
        run("md5sum -c TEST.md5")
