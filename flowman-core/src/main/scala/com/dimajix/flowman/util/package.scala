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

package com.dimajix.flowman

import scala.util.Failure
import scala.util.Success
import scala.util.Try


package object util {
    def splitSettings(settings: Seq[String]) : Seq[(String,String)] = {
        settings.map(splitSetting)
    }
    def splitSetting(setting: String) : (String,String) = {
        val sep = setting.indexOf('=')
        (setting.take(sep), setting.drop(sep + 1).trim.replaceAll("^\"|\"$","").trim)
    }

    def tryWith[A <: AutoCloseable, B](resource: A)(doWork: A => B): B = {
        try {
            doWork(resource)
        }
        finally {
            if (resource != null) {
                resource.close()
            }
        }
    }

    def TryWith[A <: AutoCloseable, B](resource: A)(doWork: A => B): Try[B] = {
        try {
            Success(doWork(resource))
        }
        catch {
            case e: Exception => Failure(e)
        }
        finally {
            if (resource != null) {
                resource.close()
            }
        }
    }
}
