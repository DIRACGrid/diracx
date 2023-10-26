FROM ghcr.io/diracgrid/diracx/server-base:latest
ARG MAMBA_DOCKERFILE_ACTIVATE=1
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

RUN pip install ./diracx git+https://github.com/DIRACGrid/DIRAC.git@integration
