# Preparing a node for container deployment

!!! warning "This is for container deployment only. For kubernetes deployment please refer to [Installing kubernetes](../install/install-kubernetes.md)."

## Podman

These instructions are using the podman container tools as they are native to Rocky9. They are very similar to docker. Please check that these are installed on your system.

## Configuring Apache

Open port 443 for apache which serves up the connection to diracx services.
Apache is configured in reverse proxy mode to forward external requests into the private container network.
An example for an apache config (/etc/httpd/conf.d/diracx.conf) file is given below:

```
ProxyPreserveHost On
ProxyPass /api http://127.0.0.1:8000/api
ProxyPassReverse /api http://127.0.0.1:8000/api
ProxyPass /.well-known http://127.0.0.1:8000/.well-known
ProxyPassReverse /.well-known http://127.0.0.1:8000/.well-known
ProxyPass / http://127.0.0.1:8001/
ProxyPassReverse / http://127.0.0.1:8001/
RequestHeader set "X-Forwarded-Proto" "https"
```

Note: We use 8000 for the diracx container and 8001 for the diracx-web container.
The same ports need to the specified in the yaml files used to steer the containers.

## Create the DiracXAuthDB

At this point we also create the DiracXAuthDB. You can refer to the documentation [here](../install/installing.md#create-the-diracxauthdb)

Note: The username and password will then re-appear in the `diracx.env` connection strings for the databases.

You can now continue to [Convert CS](../install/convert-cs.md). After this please read [Installing DiracX](../install/installing.md) for some background information: All parts, except those referencing helm or the kubernetes setup are relevant to the container deployment as well. Then proceed to [Installing DiracX in a container](installing-in-a-container.md).
