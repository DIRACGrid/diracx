FROM ghcr.io/prefix-dev/pixi:latest AS build

WORKDIR /app
COPY pixi.toml pixi.lock ./

# Copy source directories needed for path-based installs
COPY diracx-core/ diracx-core/
COPY diracx-db/ diracx-db/
COPY diracx-logic/ diracx-logic/
COPY diracx-routers/ diracx-routers/
COPY diracx-testing/ diracx-testing/
COPY pyproject.toml ./

# Switch to non-editable installs (same workaround as CI)
RUN sed -i 's@editable = true@editable = false@g' pixi.toml

RUN pixi install --locked -e container-services

# Generate activation script
RUN pixi shell-hook -e container-services > /activate.sh

FROM ubuntu:24.04

EXPOSE 8000

# Copy the installed environment
COPY --from=build /app/.pixi/envs/container-services /app/.pixi/envs/container-services
COPY --from=build /activate.sh /activate.sh

# In many clusters the container is run as a random uid for security reasons.
# If we mark the environment directory as group 0 and give it group write
# permissions then we're still able to manage the environment from inside the
# container.
RUN chmod -R g=u /app/.pixi

# Use tini as init (installed by pixi in the env)
ENTRYPOINT ["/app/.pixi/envs/container-services/bin/tini", "--", \
            "/bin/bash", "-c", "source /activate.sh && exec \"$@\"", "--"]
