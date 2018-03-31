package com.dimajix.flowman.sources.local

import java.io.File

import scala.collection.immutable.Map

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


abstract class RelationProvider {
    /**
      * Returns the short name of this relation
      * @return
      */
    def shortName() : String

    /**
      * When possible, this method should return the schema of the given `files`.  When the format
      * does not support inference, or no valid files are given should return None.  In these cases
      * Spark will require that user specify the schema manually.
      */
    def inferSchema(spark: SparkSession,
                    options: Map[String, String],
                    files: Seq[File]): Option[StructType]

    /**
      * Creates a relation for the specified parameters
      * @param spark
      * @param parameters
      * @param files - list of files to read
      * @param schema - Spark schema to use for reading/writing
      * @return
      */
    def createRelation(spark: SparkSession,
                       parameters: Map[String, String],
                       files: Seq[File],
                       schema: StructType): BaseRelation
}
