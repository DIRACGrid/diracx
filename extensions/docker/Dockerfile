FROM ghcr.io/diracgrid/diracx/services:dev

#Extension
ARG MAMBA_DOCKERFILE_ACTIVATE=1
ENV DIRACX_EXTENSIONS "mydiracx,diracx"
ENV DIRACX_DB_URL_DUMMYDB "sqlite+aiosqlite:///:memory:"

RUN pip install 'git+https://github.com/ruslan33/diracx.git@mydiracx-extension#egg=mydiracx-db&subdirectory=extensions/mydiracx/mydiracx-db'
RUN pip install 'git+https://github.com/ruslan33/diracx.git@mydiracx-extension#egg=mydiracx-routers&subdirectory=extensions/mydiracx/mydiracx-routers'
RUN pip install 'git+https://github.com/ruslan33/diracx.git@mydiracx-extension#egg=mydiracx&subdirectory=extensions/mydiracx'


