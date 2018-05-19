FROM dimajix/spark:2.2.1
MAINTAINER k.kupferschmidt@dimajix.de

ARG APP_ARCHIVE

USER root

ENV FLOMAN_HOME=/opt/flowman
ENV PATH=$PATH:$FLOMAN_HOME/bin

# Copy config files
COPY bin/ /opt/docker/bin/
COPY libexec/ /opt/docker/libexec/


# Copy and install Repository
COPY target/$APP_ARCHIVE /tmp/repo/
COPY conf/ /tmp/repo/conf
RUN tar -C /opt --owner=root --group=root -xzf /tmp/repo/$APP_ARCHIVE && \
    ln -s /opt/flowman* /opt/flowman && \
    cp -a /tmp/repo/conf/* /opt/flowman/conf && \
    chown -R root:root /opt/flowman* && \
    rm -rf /tmp/repo


ENTRYPOINT ["/opt/docker/bin/entrypoint.sh"]
CMD bash
