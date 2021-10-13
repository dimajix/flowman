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

package com.dimajix.flowman.spec.target

import java.nio.charset.Charset

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.No
import com.dimajix.common.Yes
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Target
import com.dimajix.spark.testing.LocalSparkSession
import com.dimajix.spark.testing.LocalTempDir


class MergeFilesTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "A MergeFilesTask" should "work" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context
        val fs = session.fs

        val source = fs.local(tempDir) / ("input-" + System.currentTimeMillis())
        val dest = fs.local(tempDir) / ("output-" + System.currentTimeMillis())
        dest.exists() should be (false)
        dest.isFile() should be (false)
        dest.isDirectory() should be (false)

        val file1 = (source / "file_1.txt").create()
        file1.write("This is a test".getBytes(Charset.forName("UTF-8")))
        file1.close()

        val file2 = (source / "file_2.txt").create()
        file2.write("The second line".getBytes(Charset.forName("UTF-8")))
        file2.close()

        val target = MergeFilesTarget(
            Target.Properties(context),
            source.path,
            dest.path
        )

        // == BUILD ===================================================================
        target.dirty(executor, Phase.BUILD) should be (Yes)
        target.execute(executor, Phase.BUILD)
        target.dirty(executor, Phase.BUILD) should be (No)

        dest.exists() should be (true)
        dest.isFile() should be (true)
        dest.isDirectory() should be (false)

        val in = dest.open()
        val buffer = new Array[Byte](dest.length.toInt)
        in.read(buffer)
        in.close()

        new String(buffer, "UTF-8") should be ("This is a testThe second line")

        // == VERIFY ===================================================================
        target.dirty(executor, Phase.VERIFY) should be (Yes)
        target.execute(executor, Phase.VERIFY)
        target.dirty(executor, Phase.VERIFY) should be (Yes)

        // == TRUNCATE ===================================================================
        target.dirty(executor, Phase.TRUNCATE) should be (Yes)
        target.execute(executor, Phase.TRUNCATE)
        target.dirty(executor, Phase.TRUNCATE) should be (No)

        // == DESTROY ===================================================================
        target.dirty(executor, Phase.DESTROY) should be (No)
        target.execute(executor, Phase.DESTROY)
        target.dirty(executor, Phase.DESTROY) should be (No)
    }

    it should "support delimiters" in {
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context
        val fs = session.fs

        val source = fs.local(tempDir) / ("input-" + System.currentTimeMillis())
        val dest = fs.local(tempDir) / ("output-" + System.currentTimeMillis())
        dest.exists() should be (false)
        dest.isFile() should be (false)
        dest.isDirectory() should be (false)

        val file1 = (source / "file_1.txt").create()
        file1.write("This is a test".getBytes(Charset.forName("UTF-8")))
        file1.close()

        val file2 = (source / "file_2.txt").create()
        file2.write("The second line".getBytes(Charset.forName("UTF-8")))
        file2.close()

        val target = MergeFilesTarget(
            Target.Properties(context),
            source.path,
            dest.path,
            "\n"
        )
        target.execute(executor, Phase.BUILD)

        dest.exists() should be (true)
        dest.isFile() should be (true)
        dest.isDirectory() should be (false)

        val in = dest.open()
        val buffer = new Array[Byte](dest.length.toInt)
        in.read(buffer)
        in.close()

        new String(buffer, "UTF-8") should be ("This is a test\nThe second line\n")
    }
}
