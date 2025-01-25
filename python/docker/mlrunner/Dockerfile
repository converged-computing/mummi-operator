ARG tag="ml-ucg-cpu-rocky-11-28-2024"
FROM 633731392008.dkr.ecr.us-east-1.amazonaws.com/dinosaur-de-la-noche:${tag}

COPY ./ /opt/mummi-operator
RUN . ~/.bash_profile && \
    rm -rf /opt/clones && \
    mv /opt/mummi-operator/docker/clones /opt/clones && \
    cd /opt/clones/mummi-ras && \
    pixi run --manifest-path /pixi-env/pixi.toml pip install -e .

# We don't need this for the script (we use python) but nice to have
RUN VERSION="1.2.2" && \
    curl -LO "https://github.com/oras-project/oras/releases/download/v${VERSION}/oras_${VERSION}_linux_amd64.tar.gz" && \
    mkdir -p oras-install/ && \
    tar -zxf oras_${VERSION}_*.tar.gz -C oras-install/ && \
    mv oras-install/oras /usr/local/bin/ && \
    rm -rf oras_${VERSION}_*.tar.gz oras-install/

RUN . ~/.bash_profile && \
    mv /opt/mummi-operator/docker/data/model-protein_latent_space-mini_mummi_ls-c2289_z32-t2024-10-01-23-33-17_h443704.tar.gz /opt/clones/model.tar.gz && \
    cd /opt/mummi-operator && \
    pixi run --manifest-path /pixi-env/pixi.toml pip install oras redis vim IPython && \
    pixi run --manifest-path /pixi-env/pixi.toml pip install -e .

# The new kubernetes start is provided via a jobs_mlrunner.yaml file
