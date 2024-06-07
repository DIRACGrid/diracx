FROM ghcr.io/diracgrid/diracx/services:dev

#Extension
ENV DIRACX_EXTENSIONS "gubbins,diracx"

ARG EXTRA_PACKAGES_TO_INSTALL
ARG EXTENSION_CUSTOM_SOURCES_TO_INSTALL

# RUN --mount=type=bind,source=.,target=/bindmount DIRACX_CUSTOM_SOURCE_PREFIXES=/bindmount /entrypoint.sh bash -ec "ls /bindmount/* "

# RUN /entrypoint.sh bash -ec "IFS=',' read -r -a extra_pkg_array <<< "${EXTRA_PACKAGES_TO_INSTALL}" && echo ${extra_pkg_array} && echo ${EXTRA_PACKAGES_TO_INSTALL} && echo PROUT "

RUN --mount=type=bind,source=.,target=/bindmount DIRACX_CUSTOM_SOURCE_PREFIXES=/bindmount /entrypoint.sh bash -exc "pip install --no-deps ${EXTRA_PACKAGES_TO_INSTALL} ${EXTENSION_CUSTOM_SOURCES_TO_INSTALL} && echo 'Running pip check' && pip check"



#RUN --mount=type=bind,source=.,target=/bindmount DIRACX_CUSTOM_SOURCE_PREFIXES=/bindmount /entrypoint.sh bash -ec "IFS=', ' read -r -a extra_pkg_array <<< "${EXTRA_PACKAGES_TO_INSTALL}" && echo ${extra_pkg_array} && pip install --no-deps ${EXTRA_PACKAGES_TO_INSTALL[@]} && echo 'Running pip check' && pip check"

# # In many clusters the container is ran as a random uid for security reasons.
# # If we mark the conda directory as group 0 and give it group write permissions
# # then we're still able to manage the environment from inside the container.
USER 0
RUN chown -R $MAMBA_USER:0 /opt/conda && chmod -R g=u /opt/conda
USER $MAMBA_USER
