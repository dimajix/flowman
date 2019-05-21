/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.task

import java.io.InputStream
import java.io.SequenceInputStream

import scala.util.Failure
import scala.util.Success
import scala.collection.JavaConversions._

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.hadoop.File
import com.dimajix.flowman.hadoop.FileSystem
import com.dimajix.common.tryWith
import com.dimajix.common.TryWith


object CompareFilesTask {
    def apply(context: Context, actual:Path, expected:Path) : CompareFilesTask = {
        CompareFilesTask(
            Task.Properties(context),
            actual,
            expected
        )
    }
}

case class CompareFilesTask(
    instanceProperties:Task.Properties,
    actual:Path,
    expected:Path
) extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[CompareFilesTask])

    override def execute(executor:Executor) : Boolean = {
        val fs = executor.context.fs
        val actual = fs.file(this.actual)
        val expected = fs.file(this.expected)
        logger.info(s"Checking identical content of files '$actual' and '$expected'")
        TryWith(openFile(expected)) { expectedIn =>
            tryWith(openFile(actual)) { actualIn =>
                IOUtils.contentEquals(actualIn, expectedIn)
            }
        }
        match {
            case Success(true) =>
                logger.info(s"Files '$actual' and '$expected' have the same content.")
                true
            case Success(false) =>
                logger.warn(s"Files '$actual' and '$expected' have the different content.")
                false
            case Failure(ex) =>
                logger.error(s"Caught exception while comparing file '$actual' with '$expected'", ex)
                false
        }
    }

    private def openFile(file:File) : InputStream = {
        if (file.isDirectory()) {
            val files = file.list()
                .filter(_.isFile())
                .sortBy(_.toString)
                .view
                .map(_.open())
            new SequenceInputStream(files.iterator)
        }
        else {
            file.open()
        }
    }
}



class CompareFilesTaskSpec extends TaskSpec {
    @JsonProperty(value = "actual", required = true) private var actual: String = ""
    @JsonProperty(value = "expected", required = true) private var expected: String = ""

    override def instantiate(context: Context): Task = {
        CompareFilesTask(
            instanceProperties(context),
            new Path(context.evaluate(actual)),
            new Path(context.evaluate(expected))
        )
    }
}
