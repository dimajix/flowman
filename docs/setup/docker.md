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
docker run --rm -ti dimajix/flowman:0.30.0-oss-spark3.3-hadoop3.3 bash
```
When you are using [git bash](https://git-scm.com/download/win), you will probably need to use `winpty`, which 
translates to the following command
```shell
winpty docker run --rm -ti dimajix/flowman:0.30.0-oss-spark3.3-hadoop3.3 bash
```

Once the Docker image has started, you will be presented with a bash prompt. Then you can easily build the
weather example of Flowman via
```shell
flowexec -f examples/weather job build main
```


## Mounting projects

By using Docker volumes, you can easily mount a Flowman project into the Docker container, for example

```shell
docker run --rm -ti --mount type=bind,source=$(pwd)/my_project,target=/home/flowman/my_project dimajix/flowman:0.30.0-oss-spark3.3-hadoop3.3 bash
```
The command above will start a Docker container running Flowman, and the local subdirectory `my_project` within the 
current working directory is mounted into the container at `/home/flowman/my_project`. Then you open your project
within the [Flowman Shell](../cli/flowshell/index.md) via
```shell
flowshell -f my_project
```
This way you can easily work with a normal code editor to modify the project definition in your normal file system.
Any change is immediately visible inside the Docker container, so you only need to perform a `project reload` within
the Flowman Shell to load any modifications to your project.


## Using `docker-compose`

In order to simplify working with Docker and mounting volumes, it is a good idea to use 
[Docker compose](https://docs.docker.com/compose/). A simple `docker-compose.yml` file might look as follows:
```yaml
version: "3"

services:
  flowman:
    # Set the appropriate Flowman Docker image of your choice.
    image: dimajix/flowman:0.30.0-oss-spark3.3-hadoop3.3
    # Let Docker start a bash if nothing else is specified.
    command: bash
    # Mount local volumes into the Docker container. Adjust the example entries to your needs!
    volumes:
      # Mount the local directory "conf" into the Flowman Docker container to override the Flowman configuration
      - ./conf:/opt/flowman/conf
      # Mount the local directory "my_project" into Flowmans home directory within the Docker container 
      - ./flow:/home/flowman/my_project
      # Mount the local directory "demo" into Flowmans home directory within the Docker container 
      - ./demo:/home/flowman/demo
    # Optionally set environment variables, as needed.
    environment:
      # Setup http proxy within your container
      - http_proxy=${http_proxy}
      - https_proxy=${https_proxy}
      # Setup AWS credentials within your Docker container
      - AWS_ACCESS_KEY_ID=AKIABCDEFGHIJKLMNOPQ
      - AWS_SECRET_ACCESS_KEY=1234567890abcdefghijklmn
      # Set additional environment variables used by Flowman or your project
      - SPARK_MASTER=local[10]
      - SPARK_DRIVER_MEMORY=10G
      # Pass additional options to "spark-submit", for example to use the anonymous AWS credentials provider
      - SPARK_OPTS=--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider
```
Once you have created the file `docker-compose.yml`, you then can start the Flowman Docker container via
```shell
docker-compose run --rm flowman
```
This will start a simple bash, so within the Docker container you then can start the Flowman Shell via
```shell
flowshell -f my_project
```
Of course, you can also immediately start the Flowman Shell via `docker-compose`:
```shell
docker-compose run --rm flowman flowshell -f my_project
```

Note that if you are using [git bash](https://git-scm.com/download/win) on Windows, you will probably need to use 
`winpty`, which translates to the following command:
```shell
winpty docker-compose run --rm flowman flowshell -f my_project
```



## Use Cases & Limitations

Running Flowman in a Docker image is a simple and versatile solution for performing local development tasks. This 
approach can also be a perfectly solid solution when the amount of data being processed does not justify using a 
cluster like Hadoop or Kubernetes.

On the other hand, it is not simple to connect to a cluster from within the Docker container, since Apache Spark
(the basis of Flowman) will open ports inside the container which need to be resolvable from within the cluster.
