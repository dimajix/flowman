package com.dimajix.flowman.util

import java.io.StringWriter

import scala.math.Ordering

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.velocity.VelocityContext
import org.apache.velocity.app.VelocityEngine
import org.slf4j.LoggerFactory


/**
  * Helper class for collecting files from a file system, which also support pattern substitution
  *
  * @param hadoopConf
  */
class FileCollector(hadoopConf:Configuration) {
    private val logger = LoggerFactory.getLogger(classOf[FileCollector])

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

    /**
      * Constructs a new FileCollector instance using the Hadoop configuration from the specified SparkSession
      *
      * @param spark
      * @return
      */
    def this(spark:SparkSession) = {
        this(spark.sparkContext.hadoopConfiguration)
    }

    /**
      * Sets the pattern which will be used for generating directory and/or file names from partition information
      *
      * @param pattern
      * @return
      */
    def pattern(pattern:String) : FileCollector = {
        this._pattern = pattern
        this
    }

    /**
      * Sets the base directory which is used for retrieving the file system. The base location must not contain
      * any pattern variable
      *
      * @param path
      * @return
      */
    def path(path:Path) : FileCollector = {
        this._path = path
        this
    }

    /**
      * Collects files from the given partitions
      *
      * @param partitions
      * @return
      */
    def collect(partitions:Map[String,Iterable[String]]) : Seq[Path] = {
        logger.info(s"Collecting files in location ${_path} with pattern '${_pattern}'")
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

            logger.info(s"Collecting files in location $curPath")
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
