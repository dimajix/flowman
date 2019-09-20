/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.MappingOutputIdentifier
import com.dimajix.flowman.spec.ResourceIdentifier


/**
  * This class will provide output to the local filesystem of the driver.
  */
case class LocalTarget(
    instanceProperties:Target.Properties,
    mapping:MappingOutputIdentifier,
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
            Option(namespace).map(_.name).getOrElse(""),
            Option(project).map(_.name).getOrElse(""),
            name,
            Map("filename" -> filename)
        )
    }

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    override def provides(phase: Phase) : Seq[ResourceIdentifier] = Seq(
        ResourceIdentifier.ofLocal(new Path(filename))
    )

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    override def requires(phase: Phase) : Seq[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => MappingUtils.requires(context, mapping.mapping)
            case _ => Seq()
        }
    }

    /**
      * Build the target by writing a file to the local file system of the driver
      *
      * @param executor
      */
    override def build(executor:Executor) : Unit = {
        logger.info(s"Writing mapping '${this.mapping}' to local file '$filename'")

        val mapping = context.getMapping(this.mapping.mapping)
        val dfIn = executor.instantiate(mapping, this.mapping.output)
        val cols = if (columns.nonEmpty) columns else dfIn.columns.toSeq
        val dfOut = dfIn.select(cols.map(c => dfIn(c).cast(StringType)):_*)

        val outputFile = new File(filename)
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
      * Performs a verification of the build step or possibly other checks.
      *
      * @param executor
      */
    override def verify(executor: Executor) : Unit = {
        require(executor != null)

        val file = executor.fs.local(filename)
        if (!file.exists()) {
            logger.error(s"Verification of target '$identifier' failed - local file '$filename' does not exist")
            throw new VerificationFailedException(identifier)
        }
    }

    /**
      * Cleans the target by removing the target file from the local file system
      *
      * @param executor
      */
    override def truncate(executor: Executor): Unit = {
        require(executor != null)

        val outputFile = new File(filename)
        if (outputFile.exists()) {
            logger.info(s"Cleaning local file '$filename'")
            outputFile.delete()
        }
    }

    override def destroy(executor: Executor) : Unit = {
        require(executor != null)

        val outputFile = new File(filename)
        if (outputFile.exists()) {
            logger.info(s"Removing local file '$filename'")
            outputFile.delete()
        }
    }
}



class LocalTargetSpec extends TargetSpec {
    @JsonProperty(value="input", required=true) private var input:String = _
    @JsonProperty(value="filename", required=true) private var filename:String = _
    @JsonProperty(value="encoding", required=true) private var encoding:String = "UTF-8"
    @JsonProperty(value="header", required=true) private var header:String = "true"
    @JsonProperty(value="newline", required=true) private var newline:String = "\n"
    @JsonProperty(value="delimiter", required=true) private var delimiter:String = ","
    @JsonProperty(value="quote", required=true) private var quote:String = "\""
    @JsonProperty(value="escape", required=true) private var escape:String = "\\"
    @JsonProperty(value="columns", required=true) private var columns:Seq[String] = Seq()

    override def instantiate(context: Context): LocalTarget = {
        LocalTarget(
            instanceProperties(context),
            MappingOutputIdentifier.parse(context.evaluate(input)),
            context.evaluate(filename),
            context.evaluate(encoding),
            context.evaluate(header).toBoolean,
            context.evaluate(newline),
            context.evaluate(delimiter),
            context.evaluate(quote),
            context.evaluate(escape),
            columns.map(context.evaluate)
        )
    }
}
