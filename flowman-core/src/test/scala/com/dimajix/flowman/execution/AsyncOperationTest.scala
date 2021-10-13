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


class AsyncOperationTest extends AnyFlatSpec with Matchers with MockFactory {
    "The AsyncOperation" should "work" in {
        val listener = mock[OperationListener]
        (listener.onOperationTerminated _).expects(*).once()

        val op = Operation.run("op1") {
            Thread.sleep(1000)
        }
        op.isActive should be (true)
        op.name should be ("op1")
        op.exception should be (None)
        op.addListener(listener)

        op.awaitTermination()

        op.isActive should be (false)
        op.name should be ("op1")
        op.exception should be (None)

        op.awaitTermination()

        Thread.sleep(200) // Give background thread some time to call listener
    }

    it should "catch and wrap exceptions" in {
        val listener = mock[OperationListener]
        (listener.onOperationTerminated _).expects(*).once()

        val ex = new RuntimeException("some error")

        val op = Operation.run("op1") {
            Thread.sleep(1000)
            throw ex
        }
        op.isActive should be (true)
        op.name should be ("op1")
        op.exception should be (None)
        op.addListener(listener)

        an[OperationException] should be thrownBy (op.awaitTermination())

        op.isActive should be (false)
        op.name should be ("op1")
        op.exception.get.cause should be(ex)

        an[OperationException] should be thrownBy (op.awaitTermination())

        Thread.sleep(200) // Give background thread some time to call listener
    }

    it should "catch OperationExceptions" in {
        val listener = mock[OperationListener]
        (listener.onOperationTerminated _).expects(*).once()

        val ex = new OperationException("some error")

        val op = Operation.run("op1") {
            Thread.sleep(1000)
            throw ex
        }
        op.isActive should be (true)
        op.name should be ("op1")
        op.exception should be (None)
        op.addListener(listener)

        an[OperationException] should be thrownBy (op.awaitTermination())

        op.isActive should be (false)
        op.name should be ("op1")
        op.exception.get should be(ex)

        an[OperationException] should be thrownBy (op.awaitTermination())

        Thread.sleep(200) // Give background thread some time to call listener
    }

    it should "somehow support stop()" in {
        val listener = mock[OperationListener]
        (listener.onOperationTerminated _).expects(*).once()

        val op = Operation.run("op1") {
            Thread.sleep(1000)
        }
        op.isActive should be (true)
        op.name should be ("op1")
        op.exception should be (None)
        op.addListener(listener)

        op.stop()

        op.isActive should be (false)
        op.name should be ("op1")
        op.exception should be (None)

        op.awaitTermination()
        op.stop()
        op.awaitTermination()
    }
}
