package com.dimajix.flowman.util

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession


object SparkUtils {
    def configure(spark:SparkSession, config:Map[String,String]) : Unit = {
        val sparkConf = spark.sparkContext.getConf
        config.foreach{ case(key, value) =>
            spark.conf.set(key, value)
            sparkConf.set(key, value)
        }
        val hadoopConf = spark.sparkContext.hadoopConfiguration
        if (hadoopConf != null)
            SparkHadoopUtil.get.appendS3AndSparkHadoopConfigurations(sparkConf, hadoopConf)
    }
}
