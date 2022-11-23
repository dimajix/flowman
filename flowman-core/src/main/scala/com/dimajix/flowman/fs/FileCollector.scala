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

package com.dimajix.flowman.fs

import java.io.FileNotFoundException
import java.io.StringWriter

import scala.annotation.tailrec
import scala.collection.parallel.ParIterable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.{FileSystem => HadoopFileSystem}
import org.apache.spark.sql.SparkSession
import org.apache.velocity.VelocityContext
import org.slf4j.LoggerFactory

import com.dimajix.flowman.catalog.PartitionSpec
import com.dimajix.flowman.templating.Velocity


object FileCollector {
    class Builder(hadoopConf:Configuration) {
        private var _partitions:Seq[String] = Seq()
        private var _pattern:Option[String] = None
        private var _path:Path = _
        private var _defaults:Map[String,Any] = Map()
        private var _context:VelocityContext = _

        def this(spark:SparkSession) = {
            this(spark.sparkContext.hadoopConfiguration)
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
         * @param path
         * @return
         */
        def path(path:Path) : Builder = {
            require(path != null)
            this._path = path
            this
        }

        /**
         * Creates a FileCollector with the specified configuration
         * @return
         */
        def build() : FileCollector = {
            require(_path != null)
            new FileCollector(
                hadoopConf,
                _path,
                _partitions,
                _pattern,
                _defaults
            )
        }
    }

    def builder(hadoopConf:Configuration) : Builder = new Builder(hadoopConf)
}


/**
  * Helper class for collecting files from a file system, which also support pattern substitution
  *
  * @param hadoopConf
  */
case class FileCollector(
    hadoopConf:Configuration,
    path:Path,
    partitions:Seq[String],
    pattern:Option[String],
    defaults:Map[String,Any]
) {
    private val logger = LoggerFactory.getLogger(classOf[FileCollector])

    private implicit val fileStatusOrder: Ordering[FileStatus] = new Ordering[FileStatus] {
        def compare(x: FileStatus, y: FileStatus): Int = x.getPath compareTo y.getPath
    }

    private val templateEngine = Velocity.newEngine()
    private val templateContext = Velocity.newContext()
    private val fileSystem = path.getFileSystem(hadoopConf)
    private val qualifiedPath = path.makeQualified(fileSystem.getUri, fileSystem.getWorkingDirectory)
    private val filePattern = pattern.orElse(Some(partitions.map(p => s"$p=$$$p").mkString("/"))).filter(_.nonEmpty)

    def root : Path = qualifiedPath
    def fs : org.apache.hadoop.fs.FileSystem = fileSystem

    /**
     * Checks if the root location actually exists. If the root location is a glob pattern, this method will first
     * walk up the path until no globbing component is found any more.
     * @return
     */
    def exists() : Boolean = {
        @tailrec
        def walkUp(path:Path) : Path = {
            if (path.getParent != null) {
                val p = GlobPattern(path.toString)
                if (p.hasWildcard) {
                    walkUp(path.getParent)
                }
                else {
                    path
                }
            }
            else {
                path
            }
        }

        val parent = walkUp(qualifiedPath)
        fileSystem.exists(parent)
    }

    /**
     * Resolves the root location and performs any variable substitution of the pattern with default values.
     * @return
     */
    def resolve() : Path = {
        resolve(Seq())
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolve(partition:PartitionSpec) : Path = {
        resolve(partition.toSeq)
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolve(partition:Map[String,Any]) : Path = {
        resolve(partition.toSeq)
    }

    /**
     * Resolves a single partition and performs any variable substitution.
     * @return
     */
    def resolve(partition:Seq[(String,Any)]) : Path = {
        val path = resolvePattern(partition)
        if (path.nonEmpty) {
            new Path(qualifiedPath, path)
        }
        else {
            qualifiedPath
        }
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
        filePattern.map { filePattern =>
                val partitionValues = defaults ++ partition.toMap
                try {
                    val context = new VelocityContext(templateContext)
                    partitionValues.foreach(kv => context.put(kv._1, kv._2))
                    val output = new StringWriter()
                    templateEngine.evaluate(context, output, "FileCollector", filePattern)
                    output.getBuffer.toString
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
    def collect(partitions:Iterable[PartitionSpec]) : Iterable[Path] = {
        logger.debug(s"Collecting files in location ${qualifiedPath} for multiple partitions with pattern '${filePattern.getOrElse("")}'")
        parFlatMap(partitions)((fs,p) => collectPath(fs,p,false)).toList
    }

    /**
     * Collects files from the given partitions.  The [[collect]] series
     * of methods do not perform any globbing, which means that if the [[FileCollector]] contains any globbing
     * patterns, those will be returned. Globbing-patterns which do not match (i.e. no files are found) will not
     * be returned.
     *
     * @param partitions
     * @return
     */
    def collect(partition:PartitionSpec) : Seq[Path] = {
        logger.debug(s"Collecting files in location ${qualifiedPath} for partition ${partition.spec} using pattern '${filePattern.getOrElse("")}'")
        map(partition)((fs,p) => collectPath(fs,p,false))
    }

    /**
     * Collects files from the configured directory. Does not perform partition resolution. The [[collect]] series
     * of methods do not perform any globbing, which means that if the [[FileCollector]] contains any globbing
     * patterns, those will be returned. Globbing-patterns which do not match (i.e. no files are found) will not
     * be returned.
     *
     * @return
     */
    def collect() : Seq[Path] = {
        logger.debug(s"Collecting files in location ${qualifiedPath}, for all partitions ignoring any pattern")
        map((fs,p) => collectPath(fs,p,false))
    }

    /**
     * Collects and globs files from the given partitions. Any globbing patterns will be resolved into individual
     * files and/or directories.
     *
     * @param partitions
     * @return
     */
    def glob(partitions:Iterable[PartitionSpec]) : Iterable[Path] = {
        logger.debug(s"Globbing files in location ${qualifiedPath} for multiple partitions with pattern '${filePattern.getOrElse("")}'")
        parFlatMap(partitions)((fs,p) => collectPath(fs,p,true)).toList
    }

    /**
     * Collects files from the given partitions. Any globbing patterns will be resolved into individual
     * files and/or directories.
     *
     * @param partitions
     * @return
     */
    def glob(partition:PartitionSpec) : Seq[Path] = {
        logger.debug(s"Globbing files in location ${qualifiedPath} for partition ${partition.spec} using pattern '${filePattern.getOrElse("")}'")
        map(partition)((fs,p) => collectPath(fs,p,true))
    }

    /**
     * Collects files from the configured directory. Does not perform partition resolution. Any globbing patterns will
     * be resolved into individual files and/or directories.
     *
     * @return
     */
    def glob() : Seq[Path] = {
        logger.debug(s"Globbing files in location ${qualifiedPath}, for all partitions ignoring any pattern")
        map((fs,p) => collectPath(fs,p,true))
    }

    /**
     * Creates a single globbing expression for all partitions
     * @param partitions
     * @return
     */
    def mkGlob(partitions:Iterable[PartitionSpec]) : Path = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        logger.debug(s"Globbing files in location ${qualifiedPath} for multiple partitions with pattern '${filePattern.getOrElse("")}'")
        val parts = partitions.map(p => resolvePattern(p)).mkString("{",",","}")
        new Path(qualifiedPath, parts)
    }

    /**
      * Deletes all files and directories from the given partitions
      *
      * @param partitions
      * @return
      */
    def delete(partitions:Iterable[PartitionSpec]) : Unit = {
        logger.info(s"Deleting files in location ${qualifiedPath} with pattern '${filePattern.getOrElse("")}'")
        foreach(partitions)(deletePath)
    }

    /**
      * Deletes files from the configured directory. Does not perform partition resolution
      *
      * @return
      */
    def delete() : Unit = {
        logger.info(s"Deleting files in location ${qualifiedPath}, for all partitions ignoring any pattern")
        foreach(deletePath _)
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
    def flatMap[T](partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => Iterable[T]) : Iterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.flatMap(p => fn(fileSystem, resolve(p)))
    }

    /**
      * Maps all partitions using the given function. Note that no globbing will be performed by this function.
      * @param partitions
      * @param fn
      * @tparam T
      * @return
      */
    def map[T](partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => T) : Iterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.map(p => fn(fileSystem, resolve(p)))
    }

    /**
     * Maps a single partition using the given function. Note that no globbing will be performed by this function.
     * @param partitions
     * @param fn
     * @tparam T
     * @return
     */
    def map[T](partition:PartitionSpec)(fn:(HadoopFileSystem,Path) => T) : T = {
        requirePathAndPattern()
        requireValidPartitions(partition)

        fn(fileSystem, resolve(partition))
    }

    def map[T](fn:(HadoopFileSystem,Path) => T) : T = {
        requirePath()

        fn(fileSystem,qualifiedPath)
    }

    def parFlatMap[T](partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => Iterable[T]) : ParIterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.par.flatMap(p => fn(fileSystem, resolve(p)))
    }

    def parMap[T](partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => T) : ParIterable[T] = {
        requirePathAndPattern()
        requireValidPartitions(partitions)

        partitions.par.map(p => fn(fileSystem, resolve(p)))
    }

    /**
     * Executes a specific function for a list of partitions. Note that no globbing will be performed by this function.
     * @param partitions
     * @param fn
     */
    def foreach(partitions:Iterable[PartitionSpec])(fn:(HadoopFileSystem,Path) => Unit) : Unit = {
        map(partitions)(fn)
    }

    /**
     * Executes a specific function for a list of partitions. Note that no globbing will be performed by this function.
     * @param partitions
     * @param fn
     */
    def foreach(fn:(HadoopFileSystem,Path) => Unit) : Unit = {
        map(fn)
    }

    private def truncatePath(fs:HadoopFileSystem, path:Path) : Unit = {
        val isDirectory = try fs.getFileStatus(path).isDirectory catch { case _:FileNotFoundException => false }

        if (isDirectory) {
            logger.info(s"Truncating directory '$path'")
            val files = try fs.listStatus(path) catch { case _:FileNotFoundException => null }
            if (files != null)
                files.foreach(f => fs.delete(f.getPath, true))
        }
        else {
            deletePath(fs, path)
        }
    }

    private def deletePath(fs:HadoopFileSystem, path:Path) : Unit = {
        if (!HadoopUtils.isGlobbingPattern(path)) {
          logger.info(s"Deleting directory '$path'")
          fs.delete(path, true)
        }
        else {
          logger.info(s"Deleting file(s) '$path'")
          val files = try fs.globStatus(path) catch { case _:FileNotFoundException => null }
          if (files != null)
            files.foreach { f =>
                if (!fs.delete(f.getPath, true)) {
                    logger.warn(s"Cannot delete file '${f.getPath}'")
                }
            }
        }
    }


    private def collectPath(fs:HadoopFileSystem, path:Path, performGlobbing:Boolean) : Seq[Path] = {
        if (HadoopUtils.isGlobbingPattern(path)) {
            if (performGlobbing) {
                globPath(fs, path)
            }
            else {
                globPathNonEmpty(fs, path)
            }
        }
        else {
            if (fs.exists(path))
                Seq(path)
            else
                Seq()
        }
    }

    private def globPath(fs:HadoopFileSystem, pattern: Path): Seq[Path] = {
        Option(fs.globStatus(pattern)).map { statuses =>
            statuses.map(_.getPath.makeQualified(fs.getUri, fs.getWorkingDirectory)).toSeq
        }.getOrElse(Seq.empty[Path])
    }
    private def globPathNonEmpty(fs:HadoopFileSystem, pattern: Path): Seq[Path] = {
        val nonEmpty = Option(fs.globStatus(pattern)).exists { statuses => statuses.nonEmpty }
        if (nonEmpty)
            Seq(pattern)
        else
            Seq()
    }

    private def requireValidPartitions(partitionSpec: PartitionSpec) : Unit = {
        if (!partitionSpec.values.keys.forall(partitions.contains))
            throw new IllegalArgumentException(s"Invalid entry in partition spec ${partitionSpec.spec} for partitions ${partitions.mkString(",")}")
    }
    private def requireValidPartitions(partitions: Iterable[PartitionSpec]) : Unit = {
        partitions.foreach(requireValidPartitions)
    }

    private def requirePathAndPattern() : Unit = {
        if (path.toString.isEmpty)
            throw new IllegalArgumentException("path needs to be defined for collecting partitioned files")
        if (filePattern.isEmpty)
            throw new IllegalArgumentException("pattern needs to be defined for collecting partitioned files")
    }

    private def requirePath() : Unit = {
        if (path.toString.isEmpty)
            throw new IllegalArgumentException("path needs to be defined for collecting files")
    }
}
