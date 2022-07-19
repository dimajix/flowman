FROM mcr.microsoft.com/mssql/server:2019-latest

# Create app directory
RUN mkdir -p /opt/flowman-mssql
WORKDIR /opt/flowman-mssql

USER root

COPY *.sh /opt/flowman-mssql/

RUN chown root:root /opt/flowman-mssql/* && \
    chmod a+rx /opt/flowman-mssql/*

ENTRYPOINT /bin/bash ./entrypoint.sh
