/*
 * Copyright (C) 2022 The Flowman Authors
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

package com.dimajix.flowman.spec.target

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper


class SftpUploadTargetTest extends AnyFlatSpec with Matchers {
    "A SftpUploadTarget" should "be parseable" in {
        val spec =
            """
              |kind: sftpUpload
              |source: sourceDir
              |target: targetDir
              |connection: some_connection
              |""".stripMargin

        val targetSpec = ObjectMapper.parse[TargetSpec](spec)
        targetSpec shouldBe a[SftpUploadTargetSpec]

        val session = Session.builder().disableSpark().build()
        val context = session.context

        val instance = targetSpec.instantiate(context)
        instance shouldBe a[SftpUploadTarget]

        session.shutdown()
    }
}
