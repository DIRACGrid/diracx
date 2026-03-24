FROM ghcr.io/prefix-dev/pixi:latest AS build

ARG PIXI_ENV=container-services
ARG DIRACX_EXTENSIONS=diracx

WORKDIR /app
COPY pixi.toml pixi.lock ./

# Copy source directories needed for path-based installs
COPY .git/ .git/
COPY diracx-api/ diracx-api/
COPY diracx-cli/ diracx-cli/
COPY diracx-client/ diracx-client/
COPY diracx-core/ diracx-core/
COPY diracx-db/ diracx-db/
COPY diracx-logic/ diracx-logic/
COPY diracx-routers/ diracx-routers/
COPY diracx-testing/ diracx-testing/
COPY pyproject.toml ./
COPY README.md ./
COPY extensions/gubbins/gubbins-api/ extensions/gubbins/gubbins-api/
COPY extensions/gubbins/gubbins-cli/ extensions/gubbins/gubbins-cli/
COPY extensions/gubbins/gubbins-client/ extensions/gubbins/gubbins-client/
COPY extensions/gubbins/gubbins-core/ extensions/gubbins/gubbins-core/
COPY extensions/gubbins/gubbins-db/ extensions/gubbins/gubbins-db/
COPY extensions/gubbins/gubbins-logic/ extensions/gubbins/gubbins-logic/
COPY extensions/gubbins/gubbins-routers/ extensions/gubbins/gubbins-routers/
COPY extensions/gubbins/gubbins-testing/ extensions/gubbins/gubbins-testing/
COPY extensions/gubbins/pyproject.toml extensions/gubbins/
COPY extensions/gubbins/README.md extensions/gubbins/

# Switch to non-editable installs (same workaround as CI)
RUN sed -i 's@editable = true@editable = false@g' pixi.toml

RUN pixi install --frozen -e ${PIXI_ENV}

# Generate activation script
RUN pixi shell-hook -e ${PIXI_ENV} > /activate.sh

FROM ubuntu:24.04

ARG PIXI_ENV=container-services
ARG DIRACX_EXTENSIONS=diracx
ENV PIXI_ENV=${PIXI_ENV}
ENV DIRACX_EXTENSIONS=${DIRACX_EXTENSIONS}

EXPOSE 8000

# Copy the installed environment
COPY --from=build /app/.pixi/envs/${PIXI_ENV} /app/.pixi/envs/${PIXI_ENV}
COPY --from=build /activate.sh /activate.sh
COPY containers/entrypoint.sh /entrypoint.sh

# In many clusters the container is run as a random uid for security reasons.
# If we mark the environment directory as group 0 and give it group write
# permissions then we're still able to manage the environment from inside the
# container.
RUN chmod -R g=u /app/.pixi

# Compatibility shim: the Helm chart invokes this micromamba entrypoint path
RUN printf '#!/bin/bash\nsource /activate.sh\nexec "$@"\n' > /usr/local/bin/_entrypoint.sh && \
    chmod +x /usr/local/bin/_entrypoint.sh

# Use tini as init (installed by pixi in the env)
ENTRYPOINT ["/bin/bash", "-c", \
            "exec /app/.pixi/envs/${PIXI_ENV}/bin/tini -- /entrypoint.sh \"$@\"", "--"]
