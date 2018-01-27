FROM dimajix/spark:2.2.1
MAINTAINER k.kupferschmidt@dimajix.de

USER root

# Copy and install Repository
COPY repo /opt/sxp/repo
RUN mkdir -p /opt/sxp \
    && tar -C /opt/sxp -xzf /opt/sxp/repo/sxp-datatool-1.0-dist.tar.gz \
    && tar -C /opt/sxp -xzf /opt/sxp/repo/sxp-share-1.0-dist.tar.gz

# Cleanup
RUN rm -rf /opt/sxp/repo

CMD bash
