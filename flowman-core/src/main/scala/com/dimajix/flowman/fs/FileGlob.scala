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

import java.net.URI

import scala.annotation.tailrec

import org.apache.hadoop.fs.Path

object FileGlob {
    def parse(globPath:File) : FileGlob = {
        @tailrec
        def walkUp(path: File, glob:Option[String]): FileGlob = {
            val parent = path.parent
            if (parent != null) {
                val p = GlobPattern(path.toString)
                if (p.hasWildcard) {
                    val name = path.name
                    walkUp(parent, glob.map(g => g + FileSystem.SEPARATOR + name).orElse(Some(name)))
                }
                else {
                    FileGlob(path, glob)
                }
            }
            else {
                FileGlob(path, None)
            }
        }

        walkUp(globPath, None)
    }
}
case class FileGlob(location:File, pattern:Option[String]) {
    override def toString: String = {
        pattern match {
            case Some(p) => (location / p).toString
            case None => location.toString
        }
    }

    def path : Path = {
        pattern match {
            case Some(p) => new Path(location.path, p)
            case None => location.path
        }
    }

    def /(path:String) : FileGlob = {
        pattern match {
            case Some(p) => FileGlob(location, Some(pattern + FileSystem.SEPARATOR + path))
            case None => FileGlob(location, Some(path))
        }
    }

    def file : File = {
        pattern match {
            case Some(p) => location / p
            case None => location
        }
    }

    def uri : URI = {
        file.uri
    }

    def glob() : Seq[File] = {
        pattern match {
            case Some(p) => location.glob(p)
            case None if location.exists() => Seq(location)
            case None  => Seq.empty[File]
        }
    }
    def isGlob() : Boolean = {
        pattern.exists(HadoopUtils.isGlobbingPattern)
    }
    def isEmpty : Boolean = {
        !nonEmpty
    }
    def nonEmpty : Boolean = {
        pattern match {
            case Some(p) => location.exists(p)
            case None => location.exists()
        }
    }
}
