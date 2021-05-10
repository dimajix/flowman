/*
 * Copyright 2021 Kaya Kupferschmidt
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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.spark.testing.LocalSparkSession


class ParallelExecutorTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The ParallelExecutor" should "work" in {
        val session = Session.builder().build()
        val context = session.context
        val execution = session.execution

        val targets = Seq()

        val executor = new ParallelExecutor

        executor.execute(execution, context, Phase.BUILD, targets, _ => true, keepGoing = false) {
            (execution, target, phase) => Status.SUCCESS
        }
    }
}
