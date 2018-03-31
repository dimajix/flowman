package com.dimajix.flowman.sources.local

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

    def save(path: String): Unit = {
        save(path, mode)
    }

    /**
      */
    def save(path: String, mode:SaveMode): Unit = {
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
