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

import java.io.PrintStream
import java.util.Locale

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Executor


case class PrintTask(
    instanceProperties:Task.Properties,
    output:PrintStream,
    text:Seq[String]
) extends BaseTask {
    /**
      * Abstract method which will perform the given task.
      *
      * @param executor
      */
    override def execute(executor: Executor): Boolean = {
        text.foreach(output.println)
        true
    }
}



class PrintTaskSpec extends TaskSpec {
    @JsonProperty(value = "output", required = false) private var _output: String = "stdout"
    @JsonProperty(value = "text", required = true) private var _text: Seq[String] = Seq()

    override def instantiate(context: Context): Task = {
        PrintTask(
            instanceProperties(context),
            context.evaluate(_output).toLowerCase(Locale.ROOT) match {
                case "stdout" => Console.out
                case "stderr" => Console.err
                case out:String => throw new IllegalArgumentException(s"Unsupported print output '$out'")
            },
            _text.map(context.evaluate)
        )
    }
}