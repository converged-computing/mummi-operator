ARG tag="ml-ucg-cpu-rocky-11-28-2024"
FROM 633731392008.dkr.ecr.us-east-1.amazonaws.com/dinosaur-de-la-noche:${tag}
    
COPY ./mummi-operator/python /opt/mummi-operator
RUN . ~/.bash_profile && \
    cd /opt/mummi-operator && \
    pixi run --manifest-path /pixi-env/pixi.toml pip install oras && \
    pixi run --manifest-path /pixi-env/pixi.toml pip install -e .

# This should have mummi_resources and my "mini-mummi" branch of mummi-ras
COPY ./jobs/mlrunner/entrypoint.sh ./jobs/mlrunner/kubernetes_start.sh /
COPY ./jobs/data/model-protein_latent_space-mini_mummi_ls-c2289_z32-t2024-10-01-23-33-17_h443704.tar.gz /opt/clones/model.tar.gz
