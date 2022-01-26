package com.dimajix.flowman

import com.dimajix.common.Resources


package object client {
    final private val props = Resources.loadProperties("com/dimajix/flowman/flowman.properties")
    final val JAVA_VERSION = System.getProperty("java.version")
    final val SCALA_VERSION = scala.util.Properties.versionNumberString
    final val FLOWMAN_VERSION = props.getProperty("version")
    final val SPARK_BUILD_VERSION = props.getProperty("spark_version")
    final val HADOOP_BUILD_VERSION = props.getProperty("hadoop_version")
    final val SCALA_BUILD_VERSION = props.getProperty("scala_version")
}
