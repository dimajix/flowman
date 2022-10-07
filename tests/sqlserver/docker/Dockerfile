FROM mcr.microsoft.com/mssql/server:2019-latest

USER root

# Install prerequistes since it is needed to get repo config for SQL server
RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && \
    apt-get install -y curl && \
    # curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/ubuntu/20.04/mssql-server-2019.list | tee /etc/apt/sources.list.d/mssql-server-2019.list && \
    apt-get update && \
    # Install optional packages
    apt-get install -y mssql-server-ha && \
    apt-get install -y mssql-server-fts && \
    # Cleanup the Dockerfile
    apt-get clean && \
    rm -rf /var/lib/apt/lists


# Create app directory
RUN mkdir -p /opt/flowman-mssql
WORKDIR /opt/flowman-mssql

USER root

COPY *.sh /opt/flowman-mssql/

RUN chown root:root /opt/flowman-mssql/* && \
    chmod a+rx /opt/flowman-mssql/*

ENTRYPOINT /bin/bash ./entrypoint.sh
