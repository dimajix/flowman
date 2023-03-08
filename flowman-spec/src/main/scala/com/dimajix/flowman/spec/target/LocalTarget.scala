/*
 * Copyright (C) 2018 The Flowman Authors
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

import com.dimajix.common.No
import com.dimajix.common.Trilean
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution
import com.dimajix.flowman.execution.ExecutionException
import com.dimajix.flowman.execution.MappingUtils
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetDigest


/**
  * This class will provide output to the local filesystem of the driver.
  */
case class LocalTarget(
    instanceProperties:Target.Properties,
    mapping:MappingOutputIdentifier,
    path:String,
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
    override def digest(phase:Phase) : TargetDigest = {
        TargetDigest(
            namespace.map(_.name).getOrElse(""),
            project.map(_.name).getOrElse(""),
            name,
            phase,
            Map("path" -> path)
        )
    }

    /**
     * Returns all phases which are implemented by this target in the execute method
     * @return
     */
    override def phases : Set[Phase] = Set(Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY)

    /**
      * Returns a list of physical resources produced by this target
      * @return
      */
    override def provides(phase: Phase) : Set[ResourceIdentifier] = Set(
        ResourceIdentifier.ofLocal(new Path(path))
    )

    /**
      * Returns a list of physical resources required by this target
      * @return
      */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = {
        phase match {
            case Phase.BUILD => MappingUtils.requires(context, mapping.mapping)
            case _ => Set()
        }
    }

    /**
     * Returns the state of the target, specifically of any artifacts produces. If this method return [[Yes]],
     * then an [[execute]] should update the output, such that the target is not 'dirty' any more.
     *
     * @param execution
     * @param phase
     * @return
     */
    override def dirty(execution: Execution, phase: Phase): Trilean = {
        phase match {
            case Phase.BUILD =>
                val file = execution.fs.local(path)
                !file.exists()
            case Phase.VERIFY => Yes
            case Phase.TRUNCATE|Phase.DESTROY =>
                val file = execution.fs.local(path)
                file.exists()
            case _ => No
        }
    }

    /**
      * Build the target by writing a file to the local file system of the driver
      *
      * @param executor
      */
    override def build(executor:Execution) : Unit = {
        logger.info(s"Writing mapping '${this.mapping}' to local file '$path'")

        val mapping = context.getMapping(this.mapping.mapping)
        val dfIn = executor.instantiate(mapping, this.mapping.output)
        val cols = if (columns.nonEmpty) columns else dfIn.columns.toSeq
        val dfOut = dfIn.select(cols.map(c => dfIn(c).cast(StringType)):_*)

        val outputFile = new File(path)
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

        val dfCount = countRecords(executor, dfOut)
        dfCount.rdd.toLocalIterator.foreach(row =>
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
    override def verify(executor: Execution) : Unit = {
        require(executor != null)

        val file = executor.fs.local(path)
        if (!file.exists()) {
            val error = s"Verification of target '$identifier' failed - local file '$path' does not exist"
            logger.error(error)
            throw new VerificationFailedException(identifier, new ExecutionException(error))
        }
    }

    /**
      * Cleans the target by removing the target file from the local file system
      *
      * @param executor
      */
    override def truncate(executor: Execution): Unit = {
        require(executor != null)

        val outputFile = new File(path)
        if (outputFile.exists()) {
            logger.info(s"Cleaning local file '$path'")
            outputFile.delete()
        }
    }

    override def destroy(executor: Execution) : Unit = {
        truncate(executor)
    }
}



class LocalTargetSpec extends TargetSpec {
    @JsonProperty(value="mapping", required=true) private var mapping:String = _
    @JsonProperty(value="filename", required=true) private var filename:String = _
    @JsonProperty(value="encoding", required=true) private var encoding:String = "UTF-8"
    @JsonProperty(value="header", required=true) private var header:String = "true"
    @JsonProperty(value="newline", required=true) private var newline:String = "\n"
    @JsonProperty(value="delimiter", required=true) private var delimiter:String = ","
    @JsonProperty(value="quote", required=true) private var quote:String = "\""
    @JsonProperty(value="escape", required=true) private var escape:String = "\\"
    @JsonProperty(value="columns", required=true) private var columns:Seq[String] = Seq()

    override def instantiate(context: Context, properties:Option[Target.Properties] = None): LocalTarget = {
        LocalTarget(
            instanceProperties(context, properties),
            MappingOutputIdentifier.parse(context.evaluate(mapping)),
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
