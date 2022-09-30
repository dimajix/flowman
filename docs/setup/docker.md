# Running in Docker

Flowman can also be run inside Docker, especially when working in local mode (i.e. without a cluster). This is a
great option to use in a Windows environment, where setting up a working Spark environment and running Flowman
is much more complicated (see [Running on Windows](windows.md)).

It is also
possible to run Flowman in Docker in Spark distributed processing mode, but this requires more configuration options
to forward all required ports etc.


## Starting Flowman container

We publish Flowman Docker images on [Docker Hub](https://hub.docker.com/repository/docker/dimajix/flowman),
which are good enough for local work. You can easily start a Flowman session in Docker as follows:

```shell
docker run --rm -ti dimajix/flowman:0.27.0-oss-spark3.3-hadoop3.3 bash
```
When you are using [git bash](https://git-scm.com/download/win), you will probably use `winpty`, which translates
to the following command
```shell
winpty docker run --rm -ti dimajix/flowman:0.27.0-oss-spark3.3-hadoop3.3 bash
```

Once the Docker image has started you will be presented with a bash prompt. Then you can easily build the
weather example of Flowman via
```shell
flowexec -f examples/weather job build main
```


## Mounting projects

By using Docker volumes, you can easily mount a Flowman project into the Docker container, for example

```shell
docker run --rm -ti --mount type=bind,source=$(pwd)/lessons,target=/opt/flowman/project dimajix/flowman:0.26.0-oss-spark3.2-hadoop3.3 bash
```


## Use Cases & Limitations

Running Flowman in a Docker image is a very good solution for performing local development tasks. It can also be a
perfectly solid solution when the amount of data being processed does not justify using a cluster like Hadoop or
Kubernetes.

On the other hand, it is not simple to connect to a cluster from within the Docker container, since Apache Spark
(the basis of Flowman) will open ports inside the container which need to be resolvable from within the cluster.
