/*
 * Copyright 2022 Kaya Kupferschmidt
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

package com.dimajix.flowman.spec.documentation

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.spark.testing.LocalTempDir


class FileGeneratorTest extends AnyFlatSpec with Matchers with LocalTempDir {
    "The FileGenerator" should "be parseable" in {
        val spec =
            """
              |kind: file
              |location: file:///tmp/flowman-doc
              |template: html+css
              |""".stripMargin

        val session = Session.builder()
            .disableSpark()
            .build()
        val hookSpec = ObjectMapper.parse[GeneratorSpec](spec)
        val hook = hookSpec.instantiate(session.context).asInstanceOf[FileGenerator]
        hook.location should be(new Path("file:///tmp/flowman-doc"))

        session.shutdown()
    }
}
