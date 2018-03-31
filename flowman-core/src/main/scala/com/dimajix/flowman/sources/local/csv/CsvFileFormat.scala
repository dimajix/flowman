package com.dimajix.flowman.sources.local.csv

import java.io.File

import scala.collection.immutable.Map

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.sources.local.BaseRelation
import com.dimajix.flowman.sources.local.RelationProvider


class CsvFileFormat extends RelationProvider {
    /**
      * Returns the short name of this relation
      *
      * @return
      */
    override def shortName(): String = "csv"

    /**
      * When possible, this method should return the schema of the given `files`.  When the format
      * does not support inference, or no valid files are given should return None.  In these cases
      * Spark will require that user specify the schema manually.
      */
    override def inferSchema(sparkSession: SparkSession,
                    options: Map[String, String],
                    files: Seq[File]): Option[StructType] = ???

    /**
      * Creates a relation for the specified parameters
      *
      * @param spark
      * @param parameters
      * @return
      */
    override def createRelation(spark: SparkSession,
                                parameters: Map[String, String],
                                files: Seq[File],
                                schema: StructType): BaseRelation = {
        val options = new CsvOptions(parameters)
        new CsvRelation(spark.sqlContext, files, options, schema)
    }
}
