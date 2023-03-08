/*
 * Copyright (C) 2021 The Flowman Authors
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


class ExecutorTest extends AnyFlatSpec with Matchers {
    "The Executor" should "support instantiation via reflection" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context
        val execution = session.execution
        val result = Executor.newInstance(classOf[SimpleExecutor], execution, context)

        result shouldBe a[SimpleExecutor]
    }
}
