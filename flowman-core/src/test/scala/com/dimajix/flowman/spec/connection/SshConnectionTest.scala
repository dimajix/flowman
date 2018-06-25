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

package com.dimajix.flowman.spec.connection

import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper


class SshConnectionTest extends FlatSpec with Matchers {
    "A SshConnction" should "be parseable" in {
        val spec =
            """
              |kind: ssh
              |host: my_host
            """.stripMargin
        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Connection](spec)
        result shouldBe a[SshConnection]
        val ssh = result.asInstanceOf[SshConnection]
        ssh.host should be ("my_host")
        ssh.port should be (22)
    }

    it should "be parseable as sftp" in {
        val spec =
            """
              |kind: sftp
              |host: my_host
            """.stripMargin
        val session = Session.builder().build()
        implicit val context = session.context

        val result = ObjectMapper.parse[Connection](spec)
        result shouldBe a[SshConnection]
        val ssh = result.asInstanceOf[SshConnection]
        ssh.host should be ("my_host")
        ssh.port should be (22)
    }
}
