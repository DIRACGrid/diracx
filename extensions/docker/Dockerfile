FROM ghcr.io/diracgrid/diracx/services:dev

#Extension
ARG MAMBA_DOCKERFILE_ACTIVATE=1
ENV DIRACX_EXTENSIONS "gubbins,diracx"
ENV DIRACX_DB_URL_DUMMYDB "sqlite+aiosqlite:///:memory:"

RUN pip install 'git+https://github.com/ruslan33/diracx.git@gubbins-extension#egg=gubbins-db&subdirectory=extensions/gubbins/gubbins-db'
RUN pip install 'git+https://github.com/ruslan33/diracx.git@gubbins-extension#egg=gubbins-routers&subdirectory=extensions/gubbins/gubbins-routers'
RUN pip install 'git+https://github.com/ruslan33/diracx.git@gubbins-extension#egg=gubbins&subdirectory=extensions/gubbins'
