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
import com.dimajix.flowman.fs.FileSystem.getProtocol
import com.dimajix.flowman.fs.FileSystem.stripProtocol
import com.dimajix.flowman.fs.FileSystem.stripSlash


object FileSystem {
    val SEPARATOR = "/"
    val SEPARATOR_CHAR = '/'
    val WINDOWS: Boolean = System.getProperty("os.name").startsWith("Windows")

    private val HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:")

    private def startPositionWithoutWindowsDrive(path: String) = {
        if (hasWindowsDrive(path)) {
            if (path.charAt(0) == SEPARATOR_CHAR)
                3
            else
                2
        }
        else {
            0
        }
    }

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

    def getParent(fqpath:String): String = {
        val path = stripProtocol(fqpath)
        val lastSlash = path.lastIndexOf('/')
        val start = startPositionWithoutWindowsDrive(path)
        if ((path.length == start) || (lastSlash == start && path.length == start + 1)) {
            // at root
            null
        }
        else {
            if (lastSlash == -1)
                "."
            else
                path.substring(0, if (lastSlash == start) start + 1 else lastSlash)
        }
    }

    def getProtocol(str:String) : Option[String] = {
        val colon = str.indexOf(':')
        val slash = str.indexOf('/')
        if (colon != -1 && slash != -1 && colon < slash) {
            if (hasWindowsDrive(str)) {
                None
            }
            else {
                Some(str.substring(0, colon))
            }
        }
        else {
            None
        }
    }

    def stripEndSlash(str: String) : String = {
        val colon = str.indexOf(':')
        val slash = str.lastIndexOf('/')
        if (slash == str.length - 1 && slash != colon + 1)
            str.dropRight(1)
        else
            str
    }
}


/**
  * This is a super thin wrapper around Hadoop FileSystems which is used to create Flowman File instances
  * @param conf
  */
final case class FileSystem(conf:Configuration) {

    def file(path:Path) : File = {
        val uri = path.toUri
        if (uri.getScheme == "jar") {
            resource(uri)
        }
        else {
            val fs = path.getFileSystem(conf)
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
            val jar = path.substring(colon + 1)
            val protocol = getProtocol(jar).filter(_ != "file").map(_ + ":").getOrElse("")
            val jarPath = stripProtocol(jar)
            resource(new URI("jar:" + Paths.get(protocol + jarPath).toUri.toString))
        }
        else {
            file(new Path(path))
        }
    }
    def file(uri:URI) : File = {
        if (uri.getScheme == "jar") {
            resource(uri)
        }
        else {
            // Chop off trailing "/"
            val path = uri.getPath
            val uri2 = if (path.endsWith("/")) {
                uri.resolve(path.dropRight(1))
            }
            else {
                uri
            }
            file(new Path(uri2))
        }
    }

    def local(path:Path) : File = local(path.toUri)
    def local(path:String) : File = {
        File.ofLocal(path)
    }
    def local(path:java.io.File) : File = {
        File.ofLocal(path)
    }
    def local(path:URI) : File = {
        File.ofLocal(path)
    }

    def resource(path:String) : File = {
        File.ofResource(path)
    }
    def resource(uri:URI) : File = {
        File.ofResource(uri)
    }
}
