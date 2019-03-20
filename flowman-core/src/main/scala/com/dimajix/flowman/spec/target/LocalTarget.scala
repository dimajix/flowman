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
class LocalTarget extends RelationTarget {
    private val logger = LoggerFactory.getLogger(classOf[LocalTarget])

    @JsonProperty(value="filename", required=true) private var _filename:String = _
    @JsonProperty(value="encoding", required=true) private var _encoding:String = "UTF-8"
    @JsonProperty(value="header", required=true) private var _header:String = _
    @JsonProperty(value="newline", required=true) private var _newline:String = "\n"
    @JsonProperty(value="delimiter", required=true) private var _delimiter:String = ","
    @JsonProperty(value="quote", required=true) private var _quote:String = "\""
    @JsonProperty(value="escape", required=true) private var _escape:String = "\\"
    @JsonProperty(value="columns", required=true) private var _columns:Seq[String] = _

    def filename(implicit context: Context) : String = context.evaluate(_filename)
    def encoding(implicit context: Context) : String = context.evaluate(_encoding)
    def header(implicit context: Context) : Boolean = if (_header != null) context.evaluate(_header).toBoolean else false
    def newline(implicit context: Context) : String = context.evaluate(_newline)
    def delimiter(implicit context: Context) : String = context.evaluate(_delimiter)
    def quote(implicit context: Context) : String = context.evaluate(_quote)
    def escape(implicit context: Context) : String = context.evaluate(_escape)
    def columns(implicit context: Context) : Seq[String] = if (_columns != null) _columns.map(context.evaluate) else null


    /**
      * Returns an instance representing this target with the context
      * @param context
      * @return
      */
    override def instance(implicit context: Context) : TargetInstance = {
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
        implicit var context = executor.context
        val outputFilename = this.filename
        val inputMapping = this.input
        logger.info(s"Writing mapping '$inputMapping' to local file '$outputFilename'")

        val dfIn = input(inputMapping)
        val cols = if (_columns != null && _columns.nonEmpty) columns else dfIn.columns.toSeq
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
        implicit var context = executor.context
        logger.info("Cleaning local file '{}'", filename)

        val outputFile = new File(filename)
        if (outputFile.exists()) {
            outputFile.delete()
        }
    }
}
