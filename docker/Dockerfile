FROM dimajix/spark:2.2.1
MAINTAINER k.kupferschmidt@dimajix.de

ARG APP_ARCHIVE

USER root

ENV FLOMAN_HOME=/opt/flowman
ENV PATH=$PATH:$FLOMAN_HOME/bin

# Copy config files
COPY bin/ /opt/docker/bin/
COPY libexec/ /opt/docker/libexec/
COPY conf/ /opt/docker/conf/flowman/


# Copy and install Repository
ADD target /tmp/repo/
RUN tar -C /opt --owner=root --group=root -xzf /tmp/repo/$APP_ARCHIVE && \
    ln -s /opt/flowman* /opt/flowman && \
    rm -rf /tmp/repo

ENTRYPOINT ["/opt/docker/bin/entrypoint.sh"]
CMD bash
