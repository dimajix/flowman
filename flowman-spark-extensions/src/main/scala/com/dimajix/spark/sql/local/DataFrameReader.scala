/*
 * Copyright 2018-2022 Kaya Kupferschmidt
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

package com.dimajix.spark.sql.local

import java.nio.file.Path

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


class DataFrameReader(spark:SparkSession) {
    /**
      * Specifies the input data format format.
      */
    def format(source: String): DataFrameReader = {
        this.format = source
        this
    }

    /**
      * Specifies the input schema. Some data sources can infer the input schema automatically from data.
      * By specifying the schema here, the underlying data format can skip the schema inference step, and thus
      * speed up data loading.
      */
    def schema(schema: StructType): DataFrameReader = {
        this.userSpecifiedSchema = Option(schema)
        this
    }

    /**
      * Adds an input option for the underlying data format.
      */
    def option(key: String, value: String): DataFrameReader = {
        this.extraOptions += (key -> value)
        this
    }

    /**
      * Adds an input option for the underlying data format.
      */
    def option(key: String, value: Boolean): DataFrameReader = option(key, value.toString)

    /**
      * Adds an input option for the underlying data format.
      */
    def option(key: String, value: Long): DataFrameReader = option(key, value.toString)

    /**
      * Adds an input option for the underlying data format.
      */
    def option(key: String, value: Double): DataFrameReader = option(key, value.toString)

    /**
      * Adds input options for the underlying data format.
      */
    def options(options: scala.collection.Map[String, String]): DataFrameReader = {
        this.extraOptions ++= options
        this
    }

    /**
      * Adds input options for the underlying data format.
      */
    def options(options: java.util.Map[String, String]): DataFrameReader = {
        this.options(options.asScala)
        this
    }

    /**
      * Loads input in as a `DataFrame`, for data sources that don't require a path (e.g. external
      * key-value stores).
      */
    def load(): DataFrame = {
        load(Seq.empty: _*) // force invocation of `load(...varargs...)`
    }

    /**
      * Loads input in as a `DataFrame`, for data sources that require a path (e.g. data backed by
      * a local or distributed file system).
      */
    def load(path: Path): DataFrame = {
        load(Seq(path):_*) // force invocation of `load(...varargs...)`
    }

    /**
      * Loads input in as a `DataFrame`, for data sources that support multiple paths.
      */
    def load(paths: Path*): DataFrame = {
            DataSource.apply(
                spark,
                format,
                userSpecifiedSchema,
                extraOptions.toMap).read(paths)
    }

    private var format: String = ""
    private var userSpecifiedSchema: Option[StructType] = None
    private val extraOptions = new scala.collection.mutable.HashMap[String, String]
}
