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

package com.dimajix.flowman.spec.target

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter

import com.fasterxml.jackson.annotation.JsonProperty
import com.univocity.parsers.csv.CsvFormat
import com.univocity.parsers.csv.CsvWriter
import com.univocity.parsers.csv.CsvWriterSettings
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.spec.MappingIdentifier
import com.dimajix.flowman.state.TargetInstance


/**
  * This class will provide output to the local filesystem of the driver.
  */
case class LocalTarget(
    instanceProperties:Target.Properties,
    filename:String,
    encoding:String,
    header:Boolean,
    newline:String,
    delimiter:String,
    quote:String,
    escape:String,
    columns:Seq[String]
) extends BaseTarget {
    private val logger = LoggerFactory.getLogger(classOf[LocalTarget])

    /**
      * Returns an instance representing this target with the context
      * @return
      */
    override def instance : TargetInstance = {
        TargetInstance(
            Option(context.namespace).map(_.name).getOrElse(""),
            Option(context.project).map(_.name).getOrElse(""),
            name,
            Map("filename" -> filename)
        )
    }

    /**
      * Build the target by writing a file to the local file system of the driver
      *
      * @param executor
      * @param input
      */
    override def build(executor:Executor, input:Map[MappingIdentifier,DataFrame]) : Unit = {
        val outputFilename = this.filename
        val inputMapping = instanceProperties.input
        logger.info(s"Writing mapping '$inputMapping' to local file '$outputFilename'")

        val dfIn = input(inputMapping)
        val cols = if (columns.nonEmpty) columns else dfIn.columns.toSeq
        val dfOut = dfIn.select(cols.map(c => dfIn(c).cast(StringType)):_*)

        val outputFile = new File(outputFilename)
        outputFile.getParentFile.mkdirs()
        outputFile.createNewFile
        val outputStream = new FileOutputStream(outputFile)
        val outputWriter = new OutputStreamWriter(outputStream, encoding)

        val format = new CsvFormat
        format.setLineSeparator(newline)
        format.setDelimiter(delimiter.charAt(0))
        format.setQuote(quote.charAt(0))
        format.setQuoteEscape(escape.charAt(0))
        val settings = new CsvWriterSettings
        settings.setFormat(format)

        val writer = new CsvWriter(outputWriter, settings)
        if (header)
            writer.writeHeaders(cols:_*)

        dfOut.rdd.toLocalIterator.foreach(row =>
            writer.writeRow((0 until row.size).map(row.getString).toArray)
        )

        writer.close()
        outputWriter.close()
        outputStream.close()
    }

    /**
      * Cleans the target by removing the target file from the local file system
      *
      * @param executor
      */
    override def clean(executor: Executor): Unit = {
        logger.info("Cleaning local file '{}'", filename)

        val outputFile = new File(filename)
        if (outputFile.exists()) {
            outputFile.delete()
        }
    }
}




class LocalTargetSpec extends TargetSpec {
    @JsonProperty(value="filename", required=true) private var _filename:String = _
    @JsonProperty(value="encoding", required=true) private var _encoding:String = "UTF-8"
    @JsonProperty(value="header", required=true) private var _header:String = "true"
    @JsonProperty(value="newline", required=true) private var _newline:String = "\n"
    @JsonProperty(value="delimiter", required=true) private var _delimiter:String = ","
    @JsonProperty(value="quote", required=true) private var _quote:String = "\""
    @JsonProperty(value="escape", required=true) private var _escape:String = "\\"
    @JsonProperty(value="columns", required=true) private var _columns:Seq[String] = _

    override def instantiate(context: Context): LocalTarget = {
        LocalTarget(
            instanceProperties(context),
            context.evaluate(_filename),
            context.evaluate(_encoding),
            context.evaluate(_header).toBoolean,
            context.evaluate(_newline),
            context.evaluate(_delimiter),
            context.evaluate(_quote),
            context.evaluate(_escape),
            _columns.map(context.evaluate)
        )
    }
}
