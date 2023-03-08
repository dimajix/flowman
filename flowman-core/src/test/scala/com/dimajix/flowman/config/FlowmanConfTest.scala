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

package com.dimajix.flowman.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.SimpleExecutor


class FlowmanConfTest extends AnyFlatSpec with Matchers {
    "The FlowmanConf" should "work with ints" in {
        val conf = new FlowmanConf(Map("some_int" -> "32"))
        conf.getConf(FlowmanConf.DEFAULT_TARGET_PARALLELISM) should be (16)
        conf.get("some_int") should be ("32")
    }

    it should "work with classes" in {
        val conf = new FlowmanConf(Map())
        val clazz = conf.getConf(FlowmanConf.EXECUTION_EXECUTOR_CLASS)

        clazz should be (classOf[SimpleExecutor])
    }
}
