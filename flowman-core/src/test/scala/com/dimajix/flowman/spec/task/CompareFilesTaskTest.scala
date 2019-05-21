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

package com.dimajix.flowman.spec.task

import java.io.IOException

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.testing.LocalSparkSession


class CompareFilesTaskTest extends FlatSpec with Matchers with LocalSparkSession {
    "The CompareFileTask" should "be parseable from YAML" in {
        val spec =
            """
              |kind: compareFiles
              |expected: test/data/data_1.csv
              |actual: test/data/data_1.csv
              |""".stripMargin
        val task = ObjectMapper.parse[TaskSpec](spec)
        task shouldBe a[CompareFilesTaskSpec]
    }

    it should "work on same files" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val task = CompareFilesTask(context,
            new Path("test/data/data_1.csv"),
            new Path("test/data/data_1.csv")
        )
        task.expected should be (new Path("test/data/data_1.csv"))
        task.actual should be (new Path("test/data/data_1.csv"))
        task.execute(executor) should be (true)
    }

    it should "fail on non existing actual file" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val task = CompareFilesTask(context,
            new Path("test/data/data_1.csv"),
            new Path("no_such_file")
        )
        task.expected should be (new Path("test/data/data_1.csv"))
        task.actual should be (new Path("no_such_file"))
        task.execute(executor) should be (false)
    }

    it should "throw an exception on an non existing expected file" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val task = CompareFilesTask(context,
            new Path("no_such_file"),
            new Path("test/data/data_1.csv")
        )

        task.expected should be (new Path("no_such_file"))
        task.actual should be (new Path("test/data/data_1.csv"))
        an[IOException] shouldBe thrownBy(task.execute(executor))
    }

    it should "work with a directory as expected" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val task = CompareFilesTask(context,
            new Path("test/data/data_1.csv"),
            new Path("test/data")
        )

        task.expected should be (new Path("test/data"))
        task.actual should be (new Path("test/data/data_1.csv"))
        task.execute(executor) should be (true)
    }

    it should "work with a directory as actual" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val task = CompareFilesTask(context,
            new Path("test/data"),
            new Path("test/data/data_1.csv")
        )

        task.expected should be (new Path("test/data/data_1.csv"))
        task.actual should be (new Path("test/data"))
        task.execute(executor) should be (true)
    }

    it should "work with a directory as expected and actual" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val task = CompareFilesTask(context,
            new Path("test/data/actual"),
            new Path("test/data/expected")
        )

        task.expected should be (new Path("test/data/expected"))
        task.actual should be (new Path("test/data/actual"))
        task.execute(executor) should be (true)
    }
}
