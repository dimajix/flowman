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

package com.dimajix.flowman.fs

import java.io.FileNotFoundException

import scala.collection.parallel.ParIterable
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.velocity.VelocityContext
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.templating.Velocity


object FileCollector {
    class Builder(fileSystem: FileSystem) {
        private var _partitions:Seq[String] = Seq()
        private var _pattern:Option[String] = None
        private var _location:File = _
        private var _defaults:Map[String,Any] = Map()
        private var _context:VelocityContext = _

        def this(spark:SparkSession) = {
            this(new FileSystem(spark.sparkContext.hadoopConfiguration))
        }

        /**
         * Sets the pattern which will be used for generating directory and/or file names from partition information
         *
         * @param pattern
         * @return
         */
        def pattern(pattern:String) : Builder = {
            require(pattern != null)
            this._pattern = Some(pattern)
            this
        }
        def pattern(pattern:Option[String]) : Builder = {
            require(pattern != null)
            this._pattern = pattern
            this
        }

        def partitionBy(partitions:String*) : Builder = {
            require(partitions != null)
            this._partitions = partitions
            this
        }

        /**
         * Set default values for partitions not specified in `resolve`
         * @param defaults
         * @return
         */
        def defaults(defaults:Map[String,Any]) : Builder = {
            this._defaults = defaults
            this
        }

        def context(context:VelocityContext) : Builder = {
            this._context = context
            this
        }

        /**
         * Sets the base directory which is used for retrieving the file system. The base location must not contain
         * any pattern variable
         *
         * @param location
         * @return
         */
        def location(location:Path) : Builder = {
            require(location != null)
            this._location = fileSystem.file(location)
            this
        }

        def location(location:File) : Builder = {
            require(location != null)
            this._location = location
            this
        }

        /**
         * Creates a FileCollector with the specified configuration
         * @return
         */
        def build() : FileCollector = {
            require(_location != null)
            new FileCollector(
                _location,
                _partitions,
                _pattern.orElse(Some(_partitions.map(p => s"$p=$$$p").mkString("/"))).filter(_.nonEmpty),
                _defaults
            )
        }
    }

    def builder(fs:FileSystem) : Builder = new Builder(fs)
}


/**
  * Helper class for collecting files from a file system, which also support pattern substitution
  *
  * @param hadoopConf
  */
case class FileCollector(
    location:File,
    partitions:Seq[String],
    pattern:Option[String],
    defaults:Map[String,Any]
) {
    require(pattern.nonEmpty || partitions.isEmpty)

    private val logger = LoggerFactory.getLogger(classOf[FileCollector])

    private val templateEngine = Velocity.newEngine()
    private val templateContext = Velocity.newContext()
    private val qualifiedPath = location.absolute
    private val qualifiedGlob = FileGlob.parse(qualifiedPath)

    def root : File = qualifiedPath

    /**
     * Checks if the root location actually exists. If the root location is a glob pattern, this method will first
     * walk up the path until no globbing component is found any more.
     * @return
     */
    def exists() : Boolean = {
        qualifiedGlob.location.exists()
    }

    /**
     * Resolves the root location and performs any variable substitution of the pattern with default values.
     * @return
     */
    def resolve() : FileGlob = {
        resolve(Seq.empty)
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolve(partition:PartitionSpec) : FileGlob = {
        resolve(partition.toSeq)
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolve(partition:Map[String,Any]) : FileGlob = {
        resolve(partition.toSeq)
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolve(partition:Seq[(String,Any)]) : FileGlob = {
        val path = resolvePattern(partition)
        if (path.nonEmpty)
            FileGlob(qualifiedPath, Some(path))
        else
            FileGlob(qualifiedPath, None)
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolvePattern(partition:PartitionSpec) : String = {
        resolvePattern(partition.toSeq)
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolvePattern(partition:Map[String,Any]) : String = {
        resolvePattern(partition.toSeq)
    }

    /**
     * Evaluates the pattern with the given partition
     * @param partition
     * @return
     */
    def resolvePattern(partition:Seq[(String,Any)]) : String = {
        pattern.map { filePattern =>
            val partitionValues = defaults ++ partition.toMap
            try {
                val context = Velocity.newContext(templateContext)
                partitionValues.foreach(kv => context.put(kv._1, kv._2))
                templateEngine.evaluate(context, "FileCollector", filePattern)
            }
            catch {
                case NonFatal(ex) =>
                    val parts = partitions.map(x => s"$x=${partitionValues.get(x).map(v => s"'$v'").getOrElse("<undefined>")}").mkString(",")
                    throw new IllegalArgumentException(s"Cannot evaluate partition pattern '${filePattern}' with values $parts", ex)
            }
        }
        .getOrElse("")
    }

    /**
     * Collects files from the given partitions.  The [[collect]] series
     * of methods do not perform any globbing, which means that if the [[FileCollector]] contains any globbing
     * patterns, those will be returned.  Globbing-patterns which do not match (i.e. no files are found) will not
     * be returned.
     *
     * @param partitions
     * @return
     */
    def collect(partitions:Iterable[PartitionSpec]) : Seq[File] = {
        logger.debug(s"Collecting files in location ${qualifiedPath} for multiple partitions with pattern '${pattern.getOrElse("")}'")
        parFlatMap(partitions)(collectPath).toList
    }

    /**
     * Collects files from the given partitions.  The [[collect]] series
     * of methods do not perform any globbing, which means that if the [[FileCollector]] contains any globbing
     * patterns, those will be returned. Globbing-patterns which do not match (i.e. no files are found) will not
     * be returned.
     *
     * @param partition
     * @return
     */
    def collect(partition:PartitionSpec) : Seq[File] = {
        logger.debug(s"Collecting files in location ${qualifiedPath} for partition ${partition.spec} using pattern '${pattern.getOrElse("")}'")
        map(partition)(collectPath)
    }

    /**
     * Collects files from the configured directory. Does not perform partition resolution. The [[collect]] series
     * of methods do not perform any globbing, which means that if the [[FileCollector]] contains any globbing
     * patterns, those will be returned. Globbing-patterns which do not match (i.e. no files are found) will not
     * be returned.
     *
     * @return
     */
    def collect() : Seq[File] = {
        logger.debug(s"Collecting files in location ${qualifiedPath}, for all partitions ignoring any pattern")
        collectPath(qualifiedGlob)
    }

    /**
     * Collects and globs files from the given partitions. Any globbing patterns will be resolved into individual
     * files and/or directories.
     *
     * @param partitions
     * @return
     */
    def glob(partitions:Iterable[PartitionSpec]) : Iterable[File] = {
        logger.debug(s"Globbing files in location ${qualifiedPath} for multiple partitions with pattern '${pattern.getOrElse("")}'")
        parFlatMap(partitions)(_.glob()).toList
    }

    /**
     * Collects files from the given partitions. Any globbing patterns will be resolved into individual
     * files and/or directories.
     *
     * @param partitions
     * @return
     */
    def glob(partition:PartitionSpec) : Seq[File] = {
        logger.debug(s"Globbing files in location ${qualifiedPath} for partition ${partition.spec} using pattern '${pattern.getOrElse("")}'")
        map(partition)(_.glob())
    }

    /**
     * Collects files from the configured directory. Does not perform partition resolution. Any globbing patterns will
     * be resolved into individual files and/or directories.
     *
     * @return
     */
    def glob() : Seq[File] = {
        logger.debug(s"Globbing files in location ${qualifiedPath}, for all partitions ignoring any pattern")
        qualifiedGlob.glob()
    }

    /**
      * Deletes all files and directories from the given partitions
      *
      * @param partitions
      * @return
      */
    def delete(partitions:Iterable[PartitionSpec]) : Unit = {
        logger.info(s"Deleting files in location ${qualifiedPath} with pattern '${pattern.getOrElse("")}'")
        foreach(partitions)(deletePath)
    }

    /**
      * Deletes files from the configured directory. Does not perform partition resolution
      *
      * @return
      */
    def delete() : Unit = {
        logger.info(s"Deleting files in location ${qualifiedPath}, for all partitions ignoring any pattern")
        foreach(p => deletePath(p))
    }

    /**
     * Deletes files from the configured directory. Does not perform partition resolution
     *
     * @return
     */
    def truncate() : Unit = {
        logger.info(s"Deleting files in location ${qualifiedPath}, for all partitions ignoring any pattern")
        foreach(truncatePath _)
    }

    /**
      * FlatMaps all partitions using the given function
      * @param partitions
      * @param fn
      * @tparam T
      * @return
      */
    def flatMap[T](partitions:Iterable[PartitionSpec])(fn:FileGlob => Iterable[T]) : Iterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.flatMap(p => fn(resolve(p)))
    }

    /**
      * Maps all partitions using the given function. Note that no globbing will be performed by this function.
      * @param partitions
      * @param fn
      * @tparam T
      * @return
      */
    def map[T](partitions:Iterable[PartitionSpec])(fn:FileGlob => T) : Iterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.map(p => fn(resolve(p)))
    }

    /**
     * Maps a single partition using the given function. Note that no globbing will be performed by this function.
     * @param partitions
     * @param fn
     * @tparam T
     * @return
     */
    def map[T](partition:PartitionSpec)(fn:FileGlob => T) : T = {
        requirePathAndPattern()
        requireValidPartitions(partition)

        fn(resolve(partition))
    }

    def map[T](fn:File => T) : T = {
        requirePath()

        fn(qualifiedPath)
    }

    def parFlatMap[T](partitions:Iterable[PartitionSpec])(fn:FileGlob => Iterable[T]) : ParIterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.par.flatMap(p => fn(resolve(p)))
    }

    def parMap[T](partitions:Iterable[PartitionSpec])(fn:FileGlob => T) : ParIterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.par.map(p => fn(resolve(p)))
    }

    /**
     * Executes a specific function for a list of partitions. Note that no globbing will be performed by this function.
     * @param partitions
     * @param fn
     */
    def foreach(partitions:Iterable[PartitionSpec])(fn:FileGlob => Unit) : Unit = {
        map(partitions)(fn)
    }

    /**
     * Executes a specific function for a list of partitions. Note that no globbing will be performed by this function.
     * @param partitions
     * @param fn
     */
    def foreach(fn:File => Unit) : Unit = {
        map(fn)
    }

    private def truncatePath(path:File) : Unit = {
        val isDirectory = try path.isDirectory() catch { case _:FileNotFoundException => false }

        if (isDirectory) {
            logger.info(s"Truncating directory '$path'")
            val files = try path.list() catch { case _:FileNotFoundException => Seq.empty }
            files.foreach(f => f.delete(true))
        }
        else {
            deletePath(path)
        }
    }

    private def deletePath(path:FileGlob) : Unit = {
        if (!path.isGlob()) {
          logger.info(s"Deleting directory '$path'")
          path.file.delete(true)
        }
        else {
            logger.info(s"Deleting file(s) '$path'")
            val files = try path.glob() catch {
                case _: FileNotFoundException => Seq.empty
            }
            files.foreach(deletePath)
        }
    }

    private def deletePath(path: File): Unit = {
        try {
            path.delete(true)
        }
        catch {
            case NonFatal(_) => logger.warn(s"Cannot delete file '$path'")
        }
    }


    private def collectPath(path:FileGlob) : Seq[File] = {
        // Check only if glob would result in non-empty result, and return path again
        if (path.nonEmpty)
            Seq(path.file)
        else
            Seq.empty
    }

    private def requireValidPartitions(partitionSpec: PartitionSpec) : Unit = {
        if (!partitionSpec.values.keys.forall(partitions.contains))
            throw new IllegalArgumentException(s"Invalid entry in partition spec ${partitionSpec.spec} for partitions ${partitions.mkString(",")}")
    }
    private def requireValidPartitions(partitions: Iterable[PartitionSpec]) : Unit = {
        partitions.foreach(requireValidPartitions)
    }

    private def requirePathAndPattern() : Unit = {
        if (location.toString.isEmpty)
            throw new IllegalArgumentException("path needs to be defined for collecting partitioned files")
        if (pattern.isEmpty)
            throw new IllegalArgumentException("pattern needs to be defined for collecting partitioned files")
    }

    private def requirePath() : Unit = {
        if (location.toString.isEmpty)
            throw new IllegalArgumentException("path needs to be defined for collecting files")
    }
}
