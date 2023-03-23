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

package com.dimajix.flowman.jdbc

import java.util.ServiceLoader
import scala.collection.JavaConverters._

object SqlDialects {
    private lazy val dialects = {
        val loader = ServiceLoader.load(classOf[SqlDialect])
        loader.iterator().asScala.toSeq
    }

    /**
      * Fetch the JdbcDialect class corresponding to a given database url.
      */
    def get(url: String): SqlDialect = {
        val matchingDialects = dialects.filter(_.canHandle(url))
        matchingDialects.length match {
            case 0 => NoopDialect
            case 1 => matchingDialects.head
            case _ => matchingDialects.head
        }
    }
}
