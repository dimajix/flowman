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

package com.dimajix.flowman.fs

import java.net.URI
import java.nio.file.FileSystemNotFoundException
import java.nio.file.NoSuchFileException
import java.nio.file.Paths
import java.util.Collections
import java.util.regex.Pattern

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.dimajix.common.Resources
import com.dimajix.flowman.fs.FileSystem.SEPARATOR
import com.dimajix.flowman.fs.FileSystem.stripProtocol
import com.dimajix.flowman.fs.FileSystem.stripSlash


object FileSystem {
    val SEPARATOR = "/"
    val WINDOWS: Boolean = System.getProperty("os.name").startsWith("Windows")

    private val HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:")

    def hasWindowsDrive(path: String) = FileSystem.WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(path).find

    def stripSlash(str:String) : String = {
        // Strip leading "/" before windows drive letter
        if (str.length > 1 && str(0) == '/' && hasWindowsDrive(str.substring(1))) {
            str.substring(1)
        }
        else {
            str
        }
    }
    def stripProtocol(str: String): String = {
        val colon = str.indexOf(':')
        val slash = str.indexOf('/')
        if (colon != -1 && slash != -1 && colon < slash) {
            if (hasWindowsDrive(str)) {
                str
            }
            else {
                val str2 = str.substring(colon+1)
                stripSlash(str2)
            }
        }
        else {
            str
        }
    }
}


/**
  * This is a super thin wrapper around Hadoop FileSystems which is used to create Flowman File instances
  * @param conf
  */
case class FileSystem(conf:Configuration) {

    def file(path:Path) : File = {
        if (path.toUri.getScheme == "jar") {
            resource(path.toUri)
        }
        else {
            val fs = path.getFileSystem(conf)
            val uri = path.toUri
            if (uri.getScheme == null && path.isAbsolute) {
                val p = new Path(fs.getScheme, uri.getAuthority, uri.getPath)
                HadoopFile(fs, p)
            }
            else {
                HadoopFile(fs, path)
            }
        }
    }
    def file(path:String) : File = {
        // parse uri scheme, if any
        var scheme:String = null
        val colon = path.indexOf(':')
        val slash = path.indexOf('/')
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a scheme
            scheme = path.substring(0, colon)
        }

        if (scheme == "jar") {
            resource(new URI(path))
        }
        else {
            file(new Path(path))
        }
    }
    def file(path:URI) : File = file(new Path(path))

    def local(path:Path) : File = local(path.toUri)
    def local(path:String) : File = {
        val rawPath = stripProtocol(path)
        JavaFile(Paths.get(rawPath).normalize())
    }
    def local(path:java.io.File) : File = JavaFile(path.toPath.normalize())
    def local(path:URI) : File = {
        if (path.getScheme == null) {
            val file = Paths.get(stripSlash(path.getPath))
            if (!file.isAbsolute) {
                JavaFile(file.normalize())
            }
            else {
                val uri = new URI("file", path.getUserInfo, path.getHost, path.getPort, path.getPath, path.getQuery, path.getFragment)
                JavaFile(Paths.get(uri).normalize())
            }
        }
        else {
            JavaFile(Paths.get(path).normalize())
        }
    }

    def resource(path:String) : File = {
        val url = Resources.getURL(path)
        if (url == null)
            throw new NoSuchFileException(s"Resource '$path' not found")
        resource(url.toURI)
    }
    def resource(uri:URI) : File = {
        if (uri.getScheme == "jar") {
            // Ensure JAR is opened as a file system
            try {
                java.nio.file.FileSystems.getFileSystem(uri)
            }
            catch {
                case _: FileSystemNotFoundException =>
                    java.nio.file.FileSystems.newFileSystem(uri, Collections.emptyMap[String, String]())
            }

            // Remove trailing "/", this is only present in Java 1.8
            val str = uri.toString
            val lastEx = str.lastIndexOf("!")
            val lastSep = str.lastIndexOf(SEPARATOR)
            if (lastSep == str.length - 1 && lastSep > lastEx + 1 && lastEx > 0) {
                JavaFile(Paths.get(new URI(str.dropRight(1))))
            }
            else {
                JavaFile(Paths.get(uri))
            }
        }
        else {
            JavaFile(Paths.get(uri))
        }
    }
}
