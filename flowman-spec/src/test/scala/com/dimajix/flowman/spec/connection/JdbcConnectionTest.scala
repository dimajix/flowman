/*
 * Copyright (C) 2018 The Flowman Authors
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

package com.dimajix.flowman.spec.connection

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.spec.ObjectMapper


class JdbcConnectionTest extends AnyFlatSpec with Matchers {
    "A JdbcConnction" should "be parseable" in {
        val spec =
            """
              |kind: jdbc
              |url: my_url
              |driver: my_driver
            """.stripMargin

        val result = ObjectMapper.parse[ConnectionSpec](spec)
        result shouldBe a[JdbcConnectionSpec]

        val context = RootContext.builder().build()
        val jdbc = result.instantiate(context).asInstanceOf[JdbcConnection]
        jdbc.url should be ("my_url")
        jdbc.driver should be ("my_driver")
    }

    it should "be parseable as default kind" in {
        val spec =
            """
              |url: my_url
              |driver: my_driver
            """.stripMargin

        val result = ObjectMapper.parse[ConnectionSpec](spec)
        result shouldBe a[JdbcConnectionSpec]

        val context = RootContext.builder().build()
        val jdbc = result.instantiate(context).asInstanceOf[JdbcConnection]
        jdbc.url should be ("my_url")
        jdbc.driver should be ("my_driver")
    }
}
