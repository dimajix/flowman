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

package com.dimajix.flowman.spec.storage

import org.apache.hadoop.conf.Configuration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.fs.FileSystem
import com.dimajix.spark.testing.LocalTempDir


class LocalWorkspaceTest extends AnyFlatSpec with Matchers with LocalTempDir {
    "A LocalWorkspace" should "create an empty workspace" in {
        val fs = FileSystem(new Configuration())
        val root = fs.local(tempDir) / "ws1"

        val ws1 = new LocalWorkspace(root)
        ws1.parcels should be (Seq())

        val ws2 = new LocalWorkspace(root)
        ws2.parcels should be (Seq())
    }

    it should "store parcels" in {
        val fs = FileSystem(new Configuration())
        val root = fs.local(tempDir) / "ws2"

        val ws1 = new LocalWorkspace(root)
        ws1.parcels should be (Seq())
        val p1 = LocalParcel("p1", root / "p1")
        ws1.addParcel(p1)
        ws1.parcels should be (Seq(p1))

        val ws2 = new LocalWorkspace(root)
        ws2.parcels should be (Seq(p1))
    }

    it should "correctly manage add/remove" in {
        val fs = FileSystem(new Configuration())
        val root = fs.local(tempDir) / "ws3"

        val ws = new LocalWorkspace(root)
        ws.parcels should be (Seq())
        val p1 = LocalParcel("p1", root / "p1")
        ws.addParcel(p1)
        ws.parcels should be (Seq(p1))

        an[IllegalArgumentException] should be thrownBy(ws.addParcel(p1))

        ws.removeParcel("p1")
        ws.parcels should be (Seq())
        an[IllegalArgumentException] should be thrownBy(ws.removeParcel("p1"))
    }
}
