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

package com.dimajix.flowman.execution

import scala.concurrent.ExecutionContext.Implicits.global

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class ActivityManagerTest extends AnyFlatSpec with Matchers with MockFactory {
    "The ActivityManager" should "work" in {
        val mgr = new ActivityManager
        val op = Activity.run("op1") {
            Thread.sleep(1000)
        }
        mgr.post(op)
        mgr.isActive should be (true)
        mgr.listAll().exists(_ eq op) should be (true)
        mgr.listActive().exists(_ eq op) should be (true)

        op.isActive should be (true)
        op.awaitTermination()
        op.isActive should be (false)

        Thread.sleep(200) // Give background thread some time to remove operation
        mgr.isActive should be (false)
        mgr.listAll().exists(_ eq op) should be (false)
        mgr.listActive().exists(_ eq op) should be (false)
    }

    it should "support finished activities" in {
        val mgr = new ActivityManager
        val op = Activity.run("op1") {}
        op.awaitTermination()
        op.isActive should be (false)

        mgr.post(op)
        mgr.isActive should be (false)
        mgr.listAll().exists(_ eq op) should be (false)
        mgr.listActive().exists(_ eq op) should be (false)
    }
}
