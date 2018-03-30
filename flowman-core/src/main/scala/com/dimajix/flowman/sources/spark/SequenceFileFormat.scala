package com.dimajix.flowman.sources.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.RelationProvider
import scala.collection.immutable.Map


class SequenceFileFormat extends DataSourceRegister with RelationProvider {
    override def shortName = "sequencefile"

    override def toString = "SequenceFile"

    override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
        val path = parameters.getOrElse("path", throw new IllegalArgumentException("Missing path"))
        new SequenceFileRelation(sqlContext, path)
    }
}
