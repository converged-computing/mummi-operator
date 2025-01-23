FROM 633731392008.dkr.ecr.us-east-1.amazonaws.com/dinosaur-de-la-noche:wfmanager-cpu-rocky-11-28-2024

# Update previous /opt/clones with any development changes
RUN dnf update -y && dnf install -y epel-release
RUN dnf install -y which vim htop

# Try reinstalling pixi
RUN /bin/bash -c "curl -fsSL https://pixi.sh/install.sh | bash"

# We needed to update the pixi env to have faiss and tensorflow
# I don't think we use the latter, but it is imported!
COPY ./docker/wfmanager/pixi.toml ./docker/wfmanager/pixi.lock /pixi-env/

# This should have mummi_resources and my "mini-mummi" branch of mummi-ras
COPY ./docker/wfmanager/entrypoint.sh ./docker/wfmanager/kubernetes_start.sh /
RUN . ~/.bash_profile && \
    pixi run --manifest-path /pixi-env/pixi.toml pip install IPython kubernetes redis && \
    pixi install --manifest-path /pixi-env/pixi.toml
COPY ./ /opt/mummi-operator
RUN . ~/.bash_profile && \
    cd /opt/mummi-operator && \
    pixi run --manifest-path /pixi-env/pixi.toml pip install -e .

COPY ./docker/wfmanager/wfmanager.yaml /wfmanager.yaml
