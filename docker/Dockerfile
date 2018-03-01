FROM dimajix/spark:2.2.1
MAINTAINER k.kupferschmidt@dimajix.de

ARG APP_ARCHIVE

USER root

# Copy and install Repository
ADD target /tmp/repo/
RUN tar -C /opt --owner=root --group=root -xzf /tmp/repo/$APP_ARCHIVE && \
    ln -s /opt/flowman* /opt/flowman && \
    cp -r /tmp/repo/examples /opt/flowman && \
    rm -rf /tmp/repo

ENV PATH=$PATH:/opt/flowman/bin

CMD bash
