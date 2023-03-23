/*
 * Copyright (C) 2022 The Flowman Authors
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

import java.io.InputStream
import java.io.OutputStream
import java.net.URI
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.BasicFileAttributes
import java.util.Comparator
import java.util.function.Consumer
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs

import com.dimajix.flowman.fs.FileSystem.WINDOWS


object JavaFile {
    def apply(uri:URI) : JavaFile = {
        val scheme = uri.getScheme
        if (scheme == "jar") {
            // Drop trailing "/"
            val ssp = uri.getSchemeSpecificPart
            val lastEx = ssp.lastIndexOf("!")
            val lastSep = ssp.lastIndexOf(FileSystem.SEPARATOR)
            if (lastSep == ssp.length - 1 && lastSep > lastEx + 1 && lastEx > 0) {
                new JavaFile(Paths.get(new URI(scheme, ssp.dropRight(1), null)))
            }
            else {
                new JavaFile(Paths.get(uri))
            }
        }
        else {
            new JavaFile(Paths.get(uri))
        }
    }
}

final case class JavaFile(jpath:Path) extends File {
    private lazy val _ssp = jpath.toUri.getSchemeSpecificPart.replace("file:///", "file:/")
    private lazy val _str = {
        val scheme = jpath.getFileSystem.provider().getScheme
        if (scheme == "jar") {
            "jar:" + _ssp
        }
        else {
            val rawPath =
                if (WINDOWS)
                    jpath.toString.replace('\\', '/')
                else
                    jpath.toString

            if (FileSystem.hasWindowsDrive(rawPath))
                scheme + ":/" + rawPath
            else
                scheme + ":" + rawPath
        }
    }

    override def toString: String = _str

    override def path: fs.Path = new fs.Path(uri)

    override def uri : URI = new URI(jpath.getFileSystem.provider().getScheme, _ssp, null)

    /**
     * Creates a new File object by attaching a child entry
     *
     * @param sub
     * @return
     */
    override def /(sub: String): File = {
        val uri = new URI(sub)
        if (uri.isAbsolute)
            new JavaFile(Paths.get(uri).normalize())
        else
            new JavaFile(jpath.resolve(sub))
    }

    /**
     * Returns the file name of the File
     *
     * @return
     */
    override def name : String = {
        val n = jpath.getFileName
        if (n != null) {
            // Remove trailing "/". Required for Java 1.8 (not Java 11)
            val sep = jpath.getFileSystem.getSeparator.head
            n.toString.takeWhile(_ != sep)
        } else {
            ""
        }
    }

    /**
     * Returns the parent directory of the File. If this is already a root path (i.e. no parent is available),
     * this function returns null
     *
     * @return
     */
    override def parent: File = {
        val p = jpath.getParent
        if (p != null)
            JavaFile(p)
        else
            null
    }

    /**
     * Returns the absolute path
     *
     * @return
     */
    override def absolute: File = JavaFile(jpath.toAbsolutePath)

    /**
     * Returns the size of the file. Will throw an exception if the file does not exist
     *
     * @return
     */
    override def length: Long = Files.size(jpath)

    /**
     * Lists all directory entries. Will throw an exception if the File is not a directory
     *
     * @return
     */
    override def list(): Seq[File] = Files.list(jpath)
        .collect(Collectors.toList[Path])
        .asScala
        .sortBy(_.toString)
        .map(new JavaFile(_))

    override def glob(pattern: String): Seq[File] = {
        if (pattern == "*") {
            val stream = Files.newDirectoryStream(jpath)
            try {
                stream.asScala.map(new JavaFile(_)).toList
            }
            finally {
                stream.close()
            }
        }
        else {
            val filePaths = mutable.ArrayBuffer[File]()
            val matcher = jpath.getFileSystem.getPathMatcher("glob:" + pattern)
            Files.walkFileTree(jpath, new SimpleFileVisitor[Path]() {
                override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
                    val sub = jpath.relativize(file)
                    if (matcher.matches(sub))
                        filePaths += JavaFile(file)
                    FileVisitResult.CONTINUE
                }
            })
            filePaths.toList
        }
    }

    override def exists(pattern:String) : Boolean = {
        val stream = Files.newDirectoryStream(jpath, pattern)
        try {
            stream.iterator().hasNext
        }
        finally {
            stream.close()
        }
    }

    /**
     * Renamed the file to a different name
     *
     * @param dst
     */
    override def rename(dst: fs.Path): Unit = {
        Files.move(jpath, Paths.get(dst.toUri))
    }

    /**
     * Copies the file to a different file. The relation file may reside on a different file system
     *
     * @param dst
     * @param overwrite
     */
    override def copy(dst: File, overwrite: Boolean): Unit = {
        val out = dst.create(overwrite)
        try {
            Files.copy(jpath, out)
        }
        finally {
            out.close()
        }
    }

    /**
     * Creates a file and returns the corresponding output stream. Intermediate directories will be created as required.
     *
     * @param overwrite
     * @return
     */
    override def create(overwrite: Boolean): OutputStream = {
        Files.createDirectories(jpath.getParent)
        if (overwrite)
            Files.newOutputStream(jpath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
        else
            Files.newOutputStream(jpath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    }

    override def append(): OutputStream = {
        Files.createDirectories(jpath.getParent)
        Files.newOutputStream(jpath, StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE)
    }

    /**
     * Opens an existing file and returns the corresponding input stream
     *
     * @return
     */
    override def open(): InputStream = {
        Files.newInputStream(jpath, StandardOpenOption.READ)
    }

    /**
     * Deletes the file and/or directory
     *
     * @param recursive
     */
    override def delete(recursive: Boolean): Unit = {
        if (recursive) {
            Files.walk(jpath)
                .sorted(Comparator.reverseOrder[Path]())
                .forEach(new Consumer[Path] {
                    override def accept(t: Path): Unit = t.toFile.delete()
                })
        }
        else {
            Files.delete(jpath)
        }
    }

    /**
     * Returns true if the file exists. It can either be a file or a directory
 *
     * @return
     */
    override def exists(): Boolean = Files.exists(jpath)

    override def mkdirs(): Unit = Files.createDirectories(jpath)

    /**
     * Returns true if the file exists as a directory
     *
     * @return
     */
    override def isDirectory(): Boolean = Files.isDirectory(jpath)

    /**
     * Returns true if the file exists as a normal file
     *
     * @return
     */
    override def isFile(): Boolean = Files.isRegularFile(jpath)

    /**
     * Returns true if the File is an absolute path
     *
     * @return
     */
    override def isAbsolute(): Boolean = jpath.isAbsolute

    /**
     * Creates a new File instance with an additional suffix attached. This will not physically create the file
     * on the FileSystem, but will return a File which then can be used for creation
     *
     * @param suffix
     * @return
     */
    override def withSuffix(suffix: String): File = new JavaFile(jpath.getParent.resolve(jpath.getFileName + suffix))

    override def withName(name: String): File = JavaFile(jpath.getParent.resolve(name))
}
