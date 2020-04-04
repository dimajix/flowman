/*
 * Copyright 2018-2020 Kaya Kupferschmidt
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

package com.dimajix.flowman.execution

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.model.Job
import com.dimajix.flowman.types.StringType


class JobExecutorTest extends FlatSpec with Matchers{
    "The JobExecutor" should "correctly handle environments and arguments" in {
        val session = Session.builder()
            .withEnvironment("param", "global")
            .withEnvironment("global_env", "global")
            .build()
        val context = session.context
        val job = Job.builder(context)
            .addParameter("param", StringType)
            .addEnvironment("global_env", "job")
            .addEnvironment("job_env", "job")
            .build()

        val args = Map(
            "param" -> "lala"
        )

        val executor = new JobExecutor(session.executor, job, args, force=false)
        executor.environment.toMap should be(Map(
            "param" -> "lala",
            "global_env" -> "global",
            "job_env" -> "job",
            "force" -> false
        ))
    }
}
