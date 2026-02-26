FROM ghcr.io/prefix-dev/pixi:latest AS build

WORKDIR /app
COPY pixi.toml pixi.lock ./

# Copy gubbins source directories
COPY gubbins-core/ gubbins-core/
COPY gubbins-client/ gubbins-client/
COPY gubbins-api/ gubbins-api/
COPY gubbins-cli/ gubbins-cli/
COPY gubbins-testing/ gubbins-testing/
COPY pyproject.toml ./

# Switch to non-editable installs (same workaround as CI)
RUN sed -i 's@editable = true@editable = false@g' pixi.toml

RUN pixi install --locked -e container-client

# Generate activation script
RUN pixi shell-hook -e container-client > /activate.sh

FROM ubuntu:24.04

# Copy the installed environment
COPY --from=build /app/.pixi/envs/container-client /app/.pixi/envs/container-client
COPY --from=build /activate.sh /activate.sh

# In many clusters the container is run as a random uid for security reasons.
RUN chmod -R g=u /app/.pixi

# Use tini as init (installed by pixi in the env)
ENTRYPOINT ["/app/.pixi/envs/container-client/bin/tini", "--", \
            "/bin/bash", "-c", "source /activate.sh && exec \"$@\"", "--"]
