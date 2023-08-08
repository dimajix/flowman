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

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.GlobbingResourceIdentifier
import com.dimajix.flowman.model.RegexResourceIdentifier
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.connection.SshConnection


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

    it should "correctly return requires/provides" in {
        val session = Session.builder().disableSpark().build()
        val context = session.context

        val connection = SshConnection(
            Connection.Properties(context),
            "some.host",
            22
        )

        val target = SftpUploadTarget(
            context,
            connection,
            new Path("/some/source/path"),
            new Path("/some/target/path")
        )

        target.requires(Phase.VALIDATE) should be(Set.empty)
        target.requires(Phase.CREATE) should be(Set.empty)
        target.requires(Phase.BUILD) should be(Set(GlobbingResourceIdentifier("file","/some/source/path")))
        target.requires(Phase.VERIFY) should be(Set.empty)
        target.requires(Phase.TRUNCATE) should be(Set.empty)
        target.requires(Phase.DESTROY) should be(Set.empty)

        target.provides(Phase.VALIDATE) should be(Set.empty)
        target.provides(Phase.CREATE) should be(Set.empty)
        target.provides(Phase.BUILD) should be(Set(RegexResourceIdentifier("url","sftp://some.host:22/some/target/path")))
        target.provides(Phase.VERIFY) should be(Set.empty)
        target.provides(Phase.TRUNCATE) should be(Set.empty)
        target.provides(Phase.DESTROY) should be(Set.empty)
    }
}
