FROM ghcr.io/prefix-dev/pixi:latest AS build

WORKDIR /app
COPY pixi.toml pixi.lock ./

# Copy source directories needed for path-based installs
COPY diracx-core/ diracx-core/
COPY diracx-client/ diracx-client/
COPY diracx-api/ diracx-api/
COPY diracx-cli/ diracx-cli/
COPY diracx-testing/ diracx-testing/
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
