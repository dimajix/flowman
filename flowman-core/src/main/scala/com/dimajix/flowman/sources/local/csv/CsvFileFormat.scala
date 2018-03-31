/*
 * Copyright 2018 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
