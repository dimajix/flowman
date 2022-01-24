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

package com.dimajix.spark.sql.local.csv

import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStreamWriter

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

import com.dimajix.spark.sql.local.BaseRelation


class CsvRelation(context: SQLContext, files:Seq[File], options:CsvOptions, mschema:StructType) extends BaseRelation {
    override def sqlContext: SQLContext = context

    override def schema: StructType = mschema

    override def read(): DataFrame = {
        val rows = files.flatMap(readFile)
        sqlContext.createDataFrame(rows.asJava, schema)
    }

    override def write(df: DataFrame, mode: SaveMode): Unit = {
        val outputFile = files.head
        mode match {
            case SaveMode.Overwrite =>
                outputFile.getParentFile.mkdirs()
                outputFile.createNewFile
            case SaveMode.ErrorIfExists =>
                if (outputFile.exists())
                    throw new IOException(s"File '$outputFile' already exists")
                outputFile.getParentFile.mkdirs()
                outputFile.createNewFile
            case _ =>
        }
        val outputStream = new FileOutputStream(outputFile)
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

    private def readFile(file:File) : Seq[Row] = {
        val source = Source.fromFile(file, options.encoding)
        try {
            val lines = source.getLines()
            val parser = new UnivocityReader(schema, options)
            UnivocityReader.parseIterator(lines, parser).toList
        }
        finally {
            source.close()
        }
    }
}
