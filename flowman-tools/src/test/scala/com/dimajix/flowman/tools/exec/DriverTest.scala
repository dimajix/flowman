/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.tools.exec

import org.kohsuke.args4j.CmdLineException
import org.scalatest.FlatSpec
import org.scalatest.Matchers


class DriverTest extends FlatSpec with Matchers {
    "The Driver" should "fail with an exception on wrong arguments" in {
        Driver.run() should be (true)

        a[CmdLineException] shouldBe thrownBy(Driver.run("no_such_command"))
        a[CmdLineException] shouldBe thrownBy(Driver.run("project", "run"))
    }
}
