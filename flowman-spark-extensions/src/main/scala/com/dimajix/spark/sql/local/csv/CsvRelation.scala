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

package com.dimajix.spark.sql.local.csv

import java.io.IOException
import java.io.OutputStreamWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

import com.dimajix.spark.sql.local.BaseRelation


class CsvRelation(context: SQLContext, files:Seq[Path], options:CsvOptions, mschema:StructType) extends BaseRelation {
    override def sqlContext: SQLContext = context

    override def schema: StructType = mschema

    override def read(): DataFrame = {
        val rows = files.flatMap { f =>
            if (Files.isDirectory(f))
                readDirectory(f)
            else
                readFile(f)
        }
        sqlContext.createDataFrame(rows.asJava, schema)
    }

    override def write(df: DataFrame, mode: SaveMode): Unit = {
        val outputFile = files.head
        val outputStream = mode match {
            case SaveMode.Overwrite =>
                Files.createDirectories(outputFile.getParent)
                Files.newOutputStream(outputFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
            case SaveMode.ErrorIfExists =>
                if (Files.exists(outputFile))
                    throw new IOException(s"File '$outputFile' already exists")
                Files.createDirectories(outputFile.getParent)
                Files.newOutputStream(outputFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
            case SaveMode.Append =>
                Files.createDirectories(outputFile.getParent)
                Files.newOutputStream(outputFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)
        }
        val outputWriter = new OutputStreamWriter(outputStream, options.encoding)

        val writer = new UnivocityWriter(schema, outputWriter, options)
        try {
            if (options.headerFlag) {
                writer.writeHeader()
            }

            df.rdd.toLocalIterator.foreach(writer.writeRow)
        }
        finally {
            writer.close()
            outputWriter.close()
            outputStream.close()
        }
    }

    private def readFile(file:Path) : Seq[Row] = {
        val source = Source.fromInputStream(Files.newInputStream(file, StandardOpenOption.READ), options.encoding)
        try {
            val lines = source.getLines()
            val parser = new UnivocityReader(schema, options)
            UnivocityReader.parseIterator(lines, parser).toList
        }
        finally {
            source.close()
        }
    }

    private def readDirectory(file:Path) : Seq[Row] = {
        Files.list(file)
            .collect(Collectors.toList[Path])
            .asScala
            .flatMap { f =>
                if (Files.isRegularFile(f))
                    readFile(f)
                else
                    Seq.empty[Row]
            }
    }
}
