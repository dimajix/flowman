package com.dimajix.dataflow.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import com.dimajix.dataflow.spec.model.Relation.Value


class FileCollector(hadoopConf:Configuration) {
    private var _pattern = ""
    private var _location = ""

    def this(spark:SparkSession) = {
        this(spark.sparkContext.hadoopConfiguration)
    }

    def pattern(pattern:String) : FileCollector = {
        this._pattern = pattern
        this
    }
    def location(location:String) : FileCollector = {
        this._pattern = location
        this
    }

    def collect(partitions:Map[String,Value]) : Seq[Path] = ???

}
