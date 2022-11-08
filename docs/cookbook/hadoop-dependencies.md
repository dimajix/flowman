# Hadoop Dependencies Installation

Starting with version 3.2, Spark has reduced the number of Hadoop libraries which are part of the downloadable Spark
distribution. Unfortunately, some of the libraries which have been removed are required by some Flowman plugins (for
example the S3 and Delta plugin need the `hadoop-commons` library). Since at the same time Flowman will for good
reasons not include these missing libraries, you have to install these yourself and put them into the
`$SPARK_HOME/jars` folder.


## Automated Installation

In order to simplify getting the appropriate Hadoop libraries and placing them into the correct Spark directory,
Flowman provides a small script called `install-hadoop-dependencies`, which will download and install the missing
jars:

```shell
export SPARK_HOME=your-spark-home

cd $FLOWMAN_HOME
bin/install-hadoop-dependencies
```

Note that you need to have appropriate write permissions into the `$SPARK_HOME/jars` directory, so you possibly need
to execute this with super-user privileges.

Also note that this script will download and install the Hadoop libraries with the build version of Flowman, not the
version of the already existing Hadoop libraries.
