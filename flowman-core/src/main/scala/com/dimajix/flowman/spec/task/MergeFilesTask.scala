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

package com.dimajix.flowman.spec.task

import java.nio.charset.Charset

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.hadoop.io.IOUtils
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor
import com.dimajix.flowman.hadoop.FileSystem


object MergeFilesTask {
    def apply(source:String, target:String, delimiter:String = null, overwrite:Boolean = true) : MergeFilesTask = {
        val result = new MergeFilesTask
        result._source = source
        result._target = target
        result._delimiter = delimiter
        result._overwrite = overwrite.toString
        result
    }
}


class MergeFilesTask extends BaseTask {
    private val logger = LoggerFactory.getLogger(classOf[MergeFilesTask])

    @JsonProperty(value="source", required=true) private var _source:String = ""
    @JsonProperty(value="target", required=true) private var _target:String = ""
    @JsonProperty(value="delimiter", required=true) private var _delimiter:String = _
    @JsonProperty(value="overwrite", required=false) private var _overwrite:String = "true"

    def source(implicit context:Context) : String = context.evaluate(_source)
    def target(implicit context:Context) : String = context.evaluate(_target)
    def delimiter(implicit context:Context) : String = context.evaluate(_delimiter)
    def overwrite(implicit context:Context) : Boolean = context.evaluate(_overwrite).toBoolean

    override def execute(executor:Executor) : Boolean = {
        implicit val context = executor.context
        val fs = new FileSystem(executor.hadoopConf)
        val src = fs.file(source)
        val dst = fs.local(target)
        val delimiter = Option(this.delimiter).map(_.getBytes(Charset.forName("UTF-8"))).filter(_.nonEmpty)
        logger.info(s"Merging remote files in '$src' to local file '$dst' (overwrite=$overwrite)")
        val output = dst.create(overwrite)
        try {
            fs.file(source)
                .list()
                .filter(_.isFile())
                .sortBy(_.toString)
                .foreach(file => {
                    val input = file.open()
                    try {
                        IOUtils.copyBytes(input, output, 16384, false)
                        delimiter.foreach(output.write)
                    }
                    finally {
                        input.close()
                    }
                })
        }
        finally {
            output.close()
        }
        true
    }
}
