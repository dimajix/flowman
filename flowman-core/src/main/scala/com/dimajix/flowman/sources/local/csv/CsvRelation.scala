package com.dimajix.flowman.sources.local.csv

import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStreamWriter
import java.nio.file.Files

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StructType

import com.dimajix.flowman.sources.local.BaseRelation


class CsvRelation(context: SQLContext, files:Seq[File], options:CsvOptions, mschema:StructType) extends BaseRelation {
    override def sqlContext: SQLContext = context

    override def schema: StructType = mschema

    override def read(): DataFrame = ???

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
}
