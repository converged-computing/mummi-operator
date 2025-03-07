FROM rockylinux:9
ARG MUMMI_MINI=1
ENV MUMMI_MINI=${MUMMI_MINI}

# Tell interfaces/naming we are in kubernetes, use the kubernetes-mini spec
ENV MUMMI_KUBERNETES=1

# Ensure pixi is not installed to root's home
ENV PIXI_HOME=/opt/pixi

# This corresponds with the branch to clone and directory at top level with repo
RUN /bin/bash -c "curl -fsSL https://pixi.sh/install.sh | bash"

RUN /opt/pixi/bin/pixi init pixi-env
WORKDIR /pixi-env
COPY ./ /opt/mummi-operator
RUN cp /opt/mummi-operator/docker/mlrunner/pixi.toml /pixi-env/pixi.toml && \
    cp /opt/mummi-operator/docker/mlrunner/pixi.lock /pixi-env/pixi.lock && \
    cp /opt/mummi-operator/docker/mlrunner/build.sh /pixi-env/build.sh && \
    mv /opt/mummi-operator/docker/clones /opt/clones && \
    /opt/pixi/bin/pixi install --manifest-path /pixi-env/pixi.toml

RUN /opt/pixi/bin/pixi run --manifest-path ./pixi.toml ./build.sh
RUN yum update -y && yum install -y vim rsync && \
    cd /opt/clones/mummi-ras && \
    /opt/pixi/bin/pixi run --manifest-path /pixi-env/pixi.toml pip install -e .

# We don't need this for the script (we use python) but nice to have
RUN VERSION="1.2.2" && \
    curl -LO "https://github.com/oras-project/oras/releases/download/v${VERSION}/oras_${VERSION}_linux_amd64.tar.gz" && \
    mkdir -p oras-install/ && \
    tar -zxf oras_${VERSION}_*.tar.gz -C oras-install/ && \
    mv oras-install/oras /usr/local/bin/ && \
    rm -rf oras_${VERSION}_*.tar.gz oras-install/

RUN mv /opt/mummi-operator/docker/data/model-protein_latent_space-mini_mummi_ls-c2289_z32-t2024-10-01-23-33-17_h443704.tar.gz /opt/clones/model.tar.gz

# We don't use redis, but could be a dependency somewhere
RUN cd /opt/mummi-operator && \
    /opt/pixi/bin/pixi run --manifest-path /pixi-env/pixi.toml pip install oras redis IPython && \
    /opt/pixi/bin/pixi run --manifest-path /pixi-env/pixi.toml pip install -e . && \
    /opt/pixi/bin/pixi run --manifest-path /pixi-env/pixi.toml pip install .

# /opt/clones needs to be world read/writa-able (probably just the first) for the HPC user
RUN chmod -R 777 /opt/clones
# The new kubernetes start is provided via a jobs_mlrunner.yaml file
