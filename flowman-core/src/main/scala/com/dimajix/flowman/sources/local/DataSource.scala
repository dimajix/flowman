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

package com.dimajix.flowman.sources.local

import java.io.File
import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory


case class DataSource(spark:SparkSession,
                      format:String,
                      schema:Option[StructType],
                      options: Map[String, String]) {

    private val logger = LoggerFactory.getLogger(classOf[DataSource])
    private lazy val providingClass: Class[_ <: RelationProvider] = lookupDataSource(format)

    def read(paths:Seq[String]) : DataFrame = {
        val files = paths.map(new File(_))
        val provider = providingClass.newInstance()
        val dataSchema = schema.getOrElse(provider.inferSchema(spark, options, files).get)
        val relation = provider.createRelation(spark, options, files, dataSchema)
        relation.read()
    }

    def write(path:String, df:DataFrame, mode:SaveMode) : Unit = {
        val files = Seq(new File(path))
        val provider = providingClass.newInstance()
        val dataSchema = schema.getOrElse(df.schema)
        val relation = provider.createRelation(spark, options, files, dataSchema)
        relation.write(df, mode)
    }

    private def lookupDataSource(provider: String): Class[_ <: RelationProvider] = {
        val loader = getClassloader()
        val serviceLoader = ServiceLoader.load(classOf[RelationProvider], loader)

        serviceLoader.asScala.filter(_.shortName().equalsIgnoreCase(provider)).toList match {
            case Nil =>
                throw new ClassNotFoundException(s"Cannot find class for reading '$provider'")
            case head :: Nil =>
                head.getClass
            case sources =>
                val head = sources.head.getClass
                val sourceNames = sources.map(_.getClass.getName)
                logger.warn(s"Multiple sources found for $provider (${sourceNames.mkString(", ")}), " +
                    s"defaulting to the first datasource (${head.getName}).")
                head
        }
    }

    private def getClassloader() : ClassLoader = {
        Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    }
}
