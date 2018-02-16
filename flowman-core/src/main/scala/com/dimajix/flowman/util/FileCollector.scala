package com.dimajix.flowman.util

import java.io.StringWriter

import scala.math.Ordering

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine


class FileCollector(hadoopConf:Configuration) {
    private implicit val fileStatusOrder = new Ordering[FileStatus] {
        def compare(x: FileStatus, y: FileStatus): Int = x compareTo y
    }

    private var _pattern:String = ""
    private var _path:Path = _

    private lazy val templateEngine = {
        val ve = new VelocityEngine()
        ve.init()
        ve
    }

    def this(spark:SparkSession) = {
        this(spark.sparkContext.hadoopConfiguration)
    }

    def pattern(pattern:String) : FileCollector = {
        this._pattern = pattern
        this
    }
    def path(path:Path) : FileCollector = {
        this._path = path
        this
    }

    def collect(partitions:Map[String,Iterable[String]]) : Seq[Path] = {
        val fs = _path.getFileSystem(hadoopConf)
        val context = new VelocityContext()

        def evaluate(string:String) = {
            val output = new StringWriter()
            templateEngine.evaluate(context, output, "context", string)
            output.getBuffer.toString
        }

        PartitionUtils.flatMap(partitions, p => {
            p.foreach(kv => context.put(kv._1, kv._2))
            val relPath = evaluate(_pattern)
            val curPath:Path = new Path(_path, relPath)

            if (fs.isDirectory(curPath)) {
                // If path is a directory, simply list all files
                fs.listStatus(curPath).sorted.map(_.getPath).toSeq
            }
            else {
                // Otherwise assume a file pattern and try to glob all files
                val files = fs.globStatus(curPath)
                if (files != null)
                    files.sorted.map(_.getPath).toSeq
                else
                    Seq()
            }
        }).toSeq
    }
}
