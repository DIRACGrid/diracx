FROM ghcr.io/prefix-dev/pixi:latest AS build

ARG PIXI_ENV=container-services

WORKDIR /app
COPY pixi.toml pixi.lock ./

# Copy source directories needed for path-based installs
COPY .git/ .git/
COPY gubbins-api/ gubbins-api/
COPY gubbins-cli/ gubbins-cli/
COPY gubbins-client/ gubbins-client/
COPY gubbins-core/ gubbins-core/
COPY gubbins-db/ gubbins-db/
COPY gubbins-logic/ gubbins-logic/
COPY gubbins-tasks/ gubbins-tasks/
COPY gubbins-routers/ gubbins-routers/
COPY gubbins-testing/ gubbins-testing/
COPY pyproject.toml ./
COPY README.md ./

# Switch to non-editable installs (same workaround as CI)
RUN sed -i 's@editable = true@editable = false@g' pixi.toml

RUN pixi install --frozen -e ${PIXI_ENV}

# Generate activation script
RUN pixi shell-hook -e ${PIXI_ENV} > /activate.sh

# Fetch the entrypoint script from the matching diracx version to avoid
# maintaining a duplicate copy. Dev versions fetch from main, releases
# fetch from the corresponding tag.
RUN pixi run -e ${PIXI_ENV} python -c " \
    import importlib.metadata, urllib.request; \
    v = importlib.metadata.version('diracx-core'); \
    ref = 'main' if '.dev' in v else f'v{v}'; \
    urllib.request.urlretrieve( \
        f'https://raw.githubusercontent.com/DIRACGrid/diracx/{ref}/containers/entrypoint.sh', \
        '/entrypoint.sh' \
    )" && chmod +x /entrypoint.sh

FROM ubuntu:26.04

ARG PIXI_ENV=container-services
ENV PIXI_ENV=${PIXI_ENV}

EXPOSE 8000

# Copy the installed environment
COPY --from=build /app/.pixi/envs/${PIXI_ENV} /app/.pixi/envs/${PIXI_ENV}
COPY --from=build /activate.sh /activate.sh
COPY --from=build /entrypoint.sh /entrypoint.sh

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
