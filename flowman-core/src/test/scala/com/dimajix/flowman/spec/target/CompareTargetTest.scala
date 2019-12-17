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

import org.apache.hadoop.fs.Path
import org.scalatest.FlatSpec
import org.scalatest.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.ResourceIdentifier
import com.dimajix.flowman.spec.dataset.Dataset
import com.dimajix.flowman.spec.dataset.FileDataset
import com.dimajix.spark.testing.LocalSparkSession


class CompareTargetTest extends FlatSpec with Matchers with LocalSparkSession {
    "The CompareTarget" should "be parseable from YAML" in {
        val spec =
            """
              |kind: compare
              |expected:
              |  kind: file
              |  location: test/data/data_1.csv
              |actual:
              |  kind: file
              |  location: test/data/data_1.csv
              |""".stripMargin
        val target = ObjectMapper.parse[TargetSpec](spec)
        target shouldBe a[CompareTargetSpec]
    }

    it should "work on same files" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv")
        )

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.MIGRATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set(
            ResourceIdentifier.ofFile(new Path("test/data/data_1.csv"))
        ))
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "fail on non existing actual file" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("no_such_file"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv")
        )

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.MIGRATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set(
            ResourceIdentifier.ofFile(new Path("no_such_file")),
            ResourceIdentifier.ofFile(new Path("test/data/data_1.csv"))
        ))
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        a[VerificationFailedException] shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "throw an exception on an non existing expected file" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("no_such_file"), "csv")
        )

        an[Exception] shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "work with a directory as expected" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data"), "csv")
        )

        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "work with a directory as actual" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/data_1.csv"), "csv")
        )

        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }

    it should "work with a directory as expected and actual" in {
        val session = Session.builder().build()
        val executor = session.executor
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path("test/data/actual"), "csv"),
            FileDataset(Dataset.Properties(context), new Path("test/data/expected"), "csv")
        )

        noException shouldBe thrownBy(target.execute(executor, Phase.VERIFY))
    }
}
