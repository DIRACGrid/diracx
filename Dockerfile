#TODO: replace with micromamba
FROM registry.cern.ch/docker.io/mambaorg/micromamba
EXPOSE 8000

# Create empty directory for the various volume to be mounted
# and assign them to mambauser
# This is needed for the DIRAC integration test, as docker-compose
# isn't playing well with permissions and volumes
USER root
RUN mkdir /cs_store /signing-key && chown $MAMBA_USER:$MAMBA_USER /cs_store /signing-key
USER $MAMBA_USER

WORKDIR /code

COPY --chown=$MAMBA_USER:$MAMBA_USER . /code/diracx

# openssh is needed for ssh-keygen when we generate signing key
RUN micromamba install --yes --file diracx/environment.yml --name=base git openssh &&  micromamba clean --all --yes

ARG MAMBA_DOCKERFILE_ACTIVATE=1

RUN pip install ./diracx git+https://github.com/DIRACGrid/DIRAC.git@integration
# Copying in ENTRYPOINT script
COPY --chown=$MAMBA_USER:$MAMBA_USER dockerEntrypoint.sh /
RUN chmod 755 /dockerEntrypoint.sh

# Copying the mamba specific entrypoint with lower ulimit
COPY --chown=$MAMBA_USER:$MAMBA_USER dockerMicroMambaEntrypoint.sh /
RUN chmod 755 /dockerMicroMambaEntrypoint.sh

ENTRYPOINT [ "/dockerEntrypoint.sh" ]




# activate mamba for run commands
# RUN pip install ./diracx

# CMD ["uvicorn", "--factory", "diracx.routers:create_app", "--host", "0.0.0.0", "--port", "8000"]

#  "--reload-dir", "/opt/conda/lib/python3.11/site-packages/diracx", "--reload"
