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

package com.dimajix.flowman.hadoop

import java.io.FileNotFoundException
import java.io.StringWriter

import scala.math.Ordering

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.{FileSystem => HadoopFileSystem}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.templating.Velocity


/**
  * Helper class for collecting files from a file system, which also support pattern substitution
  *
  * @param hadoopConf
  */
class FileCollector(hadoopConf:Configuration) {
    private val logger = LoggerFactory.getLogger(classOf[FileCollector])

    private implicit val fileStatusOrder = new Ordering[FileStatus] {
        def compare(x: FileStatus, y: FileStatus): Int = x.getPath compareTo y.getPath
    }

    private var _pattern:String = ""
    private var _path:Path = _
    private var _defaults:Map[String,Any] = Map()

    private lazy val templateEngine = Velocity.newEngine()
    private lazy val templateContext = Velocity.newContext()

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
     * Set default values for partitions not specified in `resolve`
     * @param defaults
     * @return
     */
    def defaults(defaults:Map[String,Any]) : FileCollector = {
        this._defaults = defaults
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

    def resolve(partition:PartitionSpec) : Path = {
        resolve(partition.toSeq)
    }
    def resolve(partition:Map[String,Any]) : Path = {
        resolve(partition.toSeq)
    }
    def resolve(partition:Seq[(String,Any)]) : Path = {
        if (_pattern != null && _pattern.nonEmpty) {
            val context = templateContext
            val partitionValues = _defaults ++ partition.toMap
            partitionValues.foreach(kv => context.put(kv._1, kv._2))
            val output = new StringWriter()
            templateEngine.evaluate(context, output, "FileCollector", _pattern)
            new Path(_path, output.getBuffer.toString)
        }
        else {
            _path
        }
    }

    /**
      * Collects files from the given partitions
      *
      * @param partitions
      * @return
      */
    def collect(partitions:Iterable[PartitionSpec]) : Seq[Path] = {
        logger.info(s"Collecting files in location ${_path} with pattern '${_pattern}'")
        flatMap(partitions)(collectPath)
    }

    def collect(partition:PartitionSpec) : Seq[Path] = {
        logger.info(s"Collecting files in location ${_path} with pattern '${_pattern}'")
        map(partition)(collectPath)
    }

    /**
      * Collects files from the configured directory. Does not perform partition resolution
      *
      * @return
      */
    def collect() : Seq[Path] = {
        logger.info(s"Collecting files in location ${_path} with pattern '${_pattern}'")
        map(collectPath)
    }

    /**
      * Deletes all files and directories from the given partitions
      *
      * @param partitions
      * @return
      */
    def delete(partitions:Iterable[PartitionSpec]) : Unit = {
        logger.info(s"Deleting files in location ${_path} with pattern '${_pattern}'")
        foreach(partitions)(deletePath)
    }

    /**
      * Deletes files from the configured directory. Does not perform partition resolution
      *
      * @return
      */
    def delete() : Unit = {
        logger.info(s"Deleting files in location ${_path} with pattern '${_pattern}'")
        foreach(deletePath _)
    }

    /**
      * FlatMaps all partitions using the given function
      * @param partitions
      * @param fn
      * @tparam T
      * @return
      */
    def flatMap[T](partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => Iterable[T]) : Seq[T] = {
        requirePathAndPattern()

        val fs = _path.getFileSystem(hadoopConf)
        partitions.flatMap(p => fn(fs, resolve(p))).toSeq
    }

    /**
      * Maps all partitions using the given function
      * @param partitions
      * @param fn
      * @tparam T
      * @return
      */
    def map[T](partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => T) : Seq[T] = {
        requirePathAndPattern()

        val fs = _path.getFileSystem(hadoopConf)
        partitions.map(p => fn(fs, resolve(p))).toSeq
    }

    def map[T](partition:PartitionSpec)(fn:(HadoopFileSystem,Path) => T) : T = {
        requirePathAndPattern()

        val fs = _path.getFileSystem(hadoopConf)
        fn(fs, resolve(partition))
    }

    def map[T](fn:(HadoopFileSystem,Path) => T) : T = {
        requirePath()

        val fs = _path.getFileSystem(hadoopConf)
        val curPath:Path = if (_pattern != null && _pattern.nonEmpty) new Path(_path, _pattern) else _path
        fn(fs,curPath)
    }

    def foreach(partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => Unit) : Unit = {
        map(partitions)(fn)
    }

    def foreach(fn:(HadoopFileSystem,Path) => Unit) : Unit = {
        map(fn)
    }

    private def deletePath(fs:HadoopFileSystem, path:Path) : Unit = {
        val isDirectory = try fs.getFileStatus(path).isDirectory catch { case _:FileNotFoundException => false }

        if (isDirectory) {
          logger.info(s"Deleting directory '$path'")
          fs.delete(path, true)
        }
        else {
          logger.info(s"Deleting file(s) '$path'")
          val files = fs.globStatus(path)
          if (files != null)
            files.foreach(f => fs.delete(f.getPath, true))
        }
    }


    private def collectPath(fs:HadoopFileSystem, path:Path) : Seq[Path] = {
        val isDirectory = try fs.getFileStatus(path).isDirectory catch { case _:FileNotFoundException => false }
        if (isDirectory) {
          logger.info(s"Collecting files in directory '$path'")
          // If path is a directory, simply list all files
          //fs.listStatus(path).sorted.map(_.getPath).toSeq
          Seq(path)
        }
        else {
          // Otherwise assume a file pattern and try to glob all files
          logger.info(s"Collecting file(s) using glob pattern '$path'")
          val files = fs.globStatus(path)
          if (files != null)
            files.sorted.map(_.getPath).toSeq
          else
            Seq()
        }
    }

    private def requirePathAndPattern() : Unit = {
        if (_path == null || _path.toString.isEmpty)
            throw new IllegalArgumentException("path needs to be defined for collecting partitioned files")
        if (_pattern == null)
            throw new IllegalArgumentException("pattern needs to be defined for collecting partitioned files")
    }

    private def requirePath() : Unit = {
        if (_path == null || _path.toString.isEmpty)
            throw new IllegalArgumentException("path needs to be defined for collecting files")
    }
}
