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
import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode


class DataFrameWriter(df:DataFrame) {
    /**
      * Specifies the input data format format.
      */
    def format(source: String): DataFrameWriter = {
        this.format = source
        this
    }

    /**
      * Adds an output option for the underlying data format.
      */
    def option(key: String, value: String): DataFrameWriter = {
        this.extraOptions += (key -> value)
        this
    }

    /**
      * Adds an output option for the underlying data format.
      */
    def option(key: String, value: Boolean): DataFrameWriter = option(key, value.toString)

    /**
      * Adds an output option for the underlying data format.
      */
    def option(key: String, value: Long): DataFrameWriter = option(key, value.toString)

    /**
      * Adds an output option for the underlying data format.
      */
    def option(key: String, value: Double): DataFrameWriter = option(key, value.toString)

    /**
      * Adds output options for the underlying data format.
      */
    def options(options: scala.collection.Map[String, String]): DataFrameWriter = {
        this.extraOptions ++= options
        this
    }

    /**
      * Adds output options for the underlying data format.
      */
    def options(options: java.util.Map[String, String]): DataFrameWriter = {
        this.options(options.asScala)
        this
    }

    def mode(mode:SaveMode) : DataFrameWriter = {
        this.mode = mode
        this
    }

    def mode(saveMode:String) : DataFrameWriter = {
        this.mode = saveMode.toLowerCase(Locale.ROOT) match {
            case "overwrite" => SaveMode.Overwrite
            case "append" => SaveMode.Append
            case "ignore" => SaveMode.Ignore
            case "error" | "default" => SaveMode.ErrorIfExists
            case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
                "Accepted save modes are 'overwrite', 'append', 'ignore', 'error'.")
        }
        this
    }

    def save(path: File): Unit = {
        save(path, mode)
    }

    /**
      */
    def save(path: File, mode:SaveMode): Unit = {
        DataSource.apply(
            df.sparkSession,
            format,
            Some(df.schema),
            extraOptions.toMap).write(path, df, mode)
    }


    private var format: String = ""
    private val extraOptions = new scala.collection.mutable.HashMap[String, String]
    private var mode:SaveMode = SaveMode.ErrorIfExists
}
