FROM dimajix/spark:2.2.1
MAINTAINER k.kupferschmidt@dimajix.de

USER root

# Copy and install Repository
ADD target /tmp/repo/
RUN ls /tmp/repo/*.tar.gz | xargs tar --owner=root --grou=root -C /opt -xzf  \
    && ln -s /opt/dataflow* /opt/dataflow \
    && rm -rf /tmp/repo

ENV PATH=$PATH:/opt/dataflow/bin

CMD bash
