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

package com.dimajix.flowman.spi

import java.util.ServiceLoader

import scala.collection.JavaConverters._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class DefaultLogFilterTest extends AnyFlatSpec with Matchers {
    "The DefaultLogFilter" should "be loadable via ServiceLoader" in {
        val logFilters = ServiceLoader.load(classOf[LogFilter]).iterator().asScala.toSeq
        logFilters.count(_.isInstanceOf[DefaultLogFilter]) should be (1)
    }

    it should "redact sensitive keys" in {
        val filter = new DefaultLogFilter
        filter.filterConfig("some_config", "some_value") should be (Some(("some_config", "some_value")))
        filter.filterConfig("MyPassword", "secret") should be (Some(("MyPassword", "***redacted***")))
        filter.filterConfig("MyPasswordId", "123") should be (Some(("MyPasswordId", "123")))
        filter.filterConfig("my.password", "secret") should be (Some(("my.password", "***redacted***")))
        filter.filterConfig("my.secret", "secret") should be (Some(("my.secret", "***redacted***")))
        filter.filterConfig("my.credential", "secret") should be (Some(("my.credential", "***redacted***")))
    }
}
