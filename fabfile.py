"""
Treeshop: The Treehouse Workshop

Experimental fabric based automation to process
a manifest and run pipelines via docker-machine.

NOTE: This is a bit of a rambling hack and very much
hard coded and idiosyncratic to the current set of
Treehouse pipelines and files they use.
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

"""
Setup the fabric hosts environment using docker-machine ip addresses as hostnames are not
resolvable. Also point to all the per machine ssh keys. An alternative would be to use one key but
on openstack the driver deletes it on termination.
"""
env.user = "ubuntu"
env.hostnames = local("docker-machine ls --filter state=Running --format '{{.Name}}'",
                      capture=True).split("\n")
env.drivernames = local("docker-machine ls --filter state=Running --format '{{.DriverName}}'",
                        capture=True).split("\n")
env.hosts = re.findall(r'[0-9]+(?:\.[0-9]+){3}',
                       local("docker-machine ls --filter state=Running --format '{{.URL}}'",
                             capture=True))
env.key_filename = ["~/.docker/machine/machines/{}/id_rsa".format(m) for m in env.hostnames]


@runs_once
def machines():
    """ Print hostname, ip, and ssh key location of each machine """
    print "Hostnames", env.hostnames
    print "Drivers:", env.drivernames
    print "IPs:", env.hosts
    print "SSH Keys:", env.key_filename


def top():
    """ Get top 5 processes on each machine """
    run("top -b -n 1 | head -n 12  | tail -n 5")


@parallel
def hello():
    """ Run echo $HOSTNAME in parallel in a container on each machine. """
    print "Running hello on {} in {}".format(env.host, env.drivernames[env.hosts.index(env.host)])
    run("docker run alpine /bin/echo ""Hello from $HOSTNAME""")


@parallel
def configure(verify="True"):
    """ Configure each machine with reference files. """
    # Put everything in data as on openstack you can't chown /mnt
    run("sudo mkdir -p /mnt/data")
    run("sudo chown ubuntu:ubuntu /mnt/data")
    run("mkdir -p /mnt/data/references")
    run("mkdir -p /mnt/data/samples")
    run("mkdir -p /mnt/data/outputs")

    # Grab references from the local blob store to each environment
    if env.drivernames[env.hosts.index(env.host)] is "azure":
        base_ref_url = "https://treeshop.blob.core.windows.net/references"
    else:
        base_ref_url = "http://ceph-gw-01.pod/references"

    with cd("/mnt/data/references"):
        for ref in ["kallisto_hg38.idx",
                    "starIndex_hg38_no_alt.tar.gz",
                    "rsem_ref_hg38_no_alt.tar.gz",
                    "STARFusion-GRCh38gencode23.tar.gz"]:
            if not exists(ref):
                run("wget -nv -N {}/{}".format(base_ref_url, ref))
        if not exists("STARFusion-GRCh38gencode23"):
            run("tar -xvf STARFusion-GRCh38gencode23.tar.gz")
        if verify == "True":
            put("refs.rnaseq.md5", "/mnt/data/references")
            run("md5sum -c refs.rnaseq.md5")
            put("refs.fusion.md5", "/mnt/data/references")
            run("md5sum -c refs.fusion.md5")


def _run_rnaseq(r1, r2, name, prune):
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


def _run_qc(bam, prune):
    run("mkdir outputs/qc")
    run("""
        docker run --rm --name qc \
            -v /mnt/data/outputs/qc:/data \
            -v {}:/data/rnaAligned.sortedByCoord.out.bam \
            hbeale/treehouse_bam_qc:1.0 runQC.sh
        """.format(bam))
    # prune
    if prune:
        run("rm -f outputs/qc/rnaAligned.sortedByName.bam")
        run("rm -f outputs/qc/rnaAligned.sortedByName.md.bam")
        run("rm -f outputs/qc/rnaAligned.sortedByCoord.out.bam")
    return "hbeale/treehouse_bam_qc:1.0"


def _run_fusion(r1, r2, prune):
    run("mkdir outputs/fusion")
    run("""
        docker run --rm --name fusion \
            -v /mnt/data:/data \
            jpfeil/star-fusion:0.0.1 \
            --CPU `nproc` \
            --genome_lib_dir references/STARFusion-GRCh38gencode23 \
            --left_fq samples/{} --right_fq samples/{} --output_dir outputs/fusion
        """.format(r1, r2))
    # prune
    if prune:
        run("rm -f outputs/fusion/*.bam")
    return "jpfeil/star-fusion:0.0.1"


def reset_machine():
    # Stop any existing processing and delete inputs and outputs
    with warn_only():
        run("docker stop rnaseq && docker rm rnaseq")
        run("docker stop fusion && docker rm fusion")
        run("docker stop qc && docker rm qc")
        sudo("rm -rf /mnt/data/samples/*")
        sudo("rm -rf /mnt/data/outputs/*")


@parallel
def process(manifest, outputs=".",
            rnaseq="True", qc="True", fusion="True",
            prune="True", limit=None):
    """ Process on all the samples in 'manifest' """

    def log_error(message):
        print message
        with open("{}/errors.txt".format(outputs), "a") as error_log:
            error_log.write(message + "\n")

    print "Processing starting on {}".format(env.host)

    # Each machine will process every #hosts samples
    for sample in itertools.islice(csv.DictReader(open(manifest, "rU"), delimiter="\t"),
                                   env.hosts.index(env.host),
                                   int(limit) if limit else None, len(env.hosts)):
        sample_id = sample["Submitter Sample ID"]
        sample_files = sample["File Path"].split(",")
        print "{} processing {}".format(env.host, sample_id)

        if os.path.exists("{}/{}".format(outputs, sample_id)):
            log_error("{}/{} already exists".format(outputs, sample_id))
            continue

        # See if all the files exist
        for sample in sample_files:
            if not os.path.isfile(sample):
                log_error("{} does not exist".format(sample))
                continue

            # Hack to make sure sample name and file name match because RNASeq
            # puts the file name as the gene_id in the RSEM file and MedBook
            # uses that to name the sample.
            if rnaseq == "True" and not os.path.basename(sample).startswith(sample_id):
                log_error("Filename does not match sample id: {} {}".format(sample_id, sample))
                continue

        print "Resetting {}".format(env.host)
        reset_machine()

        methods = {"user": os.environ["USER"],
                   "start": datetime.datetime.utcnow().isoformat(),
                   "treeshop_version": local(
                       "git --work-tree={0} --git-dir {0}/.git describe --always".format(
                           os.path.dirname(__file__)), capture=True),
                   "inputs": sample_files,
                   "pipelines": []}

        with cd("/mnt/data"):
            # Copy fastqs, fixing r1/r2 for R1/R2 if needed
            if (rnaseq == "True") or (fusion == "True"):
                if len(sample_files) != 2:
                    log_error("Expected 2 samples files {} {}".format(sample_id, sample_files))
                    continue

                for fastq in sample_files:
                    if not os.path.isfile(fastq):
                        log_error("Unable to find file: {} {}".format(sample_id, fastq))
                        continue
                    if not exists("samples/{}".format(os.path.basename(fastq))):
                        print "Copying files...."
                        put(fastq, "samples/{}".format(
                            os.path.basename(fastq).replace("r1.", "R1.").replace("r2.", "R2.")))

                r1, r2 = [os.path.basename(f).replace("r1.", "R1.").replace("r2.", "R2.")
                          for f in sample_files]

            # If only running qc then copy bam as if it came from rnaseq
            if (qc == "True") and (rnaseq != "True") and (fusion != "True"):
                if not sample_files[0].endswith(".bam"):
                    log_error("Expected bam for {} {}".format(sample_id, sample_files))
                    continue
                print "Copying bam for {}".format(sample_id)
                put(sample_files[0],
                    "outputs/{}.sorted.bam".format(sample_id))

            # Create folder on storage for results named after sample id
            # Wait until now in case something above fails so we don't have
            # an empty directory
            results = "{}/{}".format(outputs, sample_id)
            local("mkdir -p {}".format(results))

            # rnaseq
            if rnaseq == "True":
                methods["pipelines"].append(_run_rnaseq(r1, r2, sample_id, prune == "True"))
                get("outputs/rnaseq", results)
                if prune != "True":
                    get("/mnt/data/outputs/{}.sorted.bam".format(sample_id), results)

            # qc on rnaseq output bam
            if qc == "True" or rnaseq == "True":  # qc ALWAYS if rnaseq
                print "Starting qc for {}".format(sample_id)
                methods["pipelines"].append(
                    _run_qc("/mnt/data/outputs/{}.sorted.bam".format(sample_id), prune == "True"))
                get("outputs/qc", results)

            # fusion
            if fusion == "True":
                methods["pipelines"].append(_run_fusion(r1, r2, prune == "True"))
                get("outputs/fusion", results)

        # Write out methods
        methods["end"] = datetime.datetime.utcnow().isoformat()
        with open("{}/methods.json".format(results), "w") as f:
            f.write(json.dumps(methods, indent=4))


def verify():
    # Verify md5 of rnaseq output from TEST samples
    with cd("/mnt/data/outputs"):
        put("TEST.md5", "/mnt/data/outputs")
        run("md5sum -c TEST.md5")
