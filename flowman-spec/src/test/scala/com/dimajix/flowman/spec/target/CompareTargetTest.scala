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

package com.dimajix.flowman.spec.target

import com.google.common.io.Resources
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.execution.VerificationFailedException
import com.dimajix.flowman.model.Dataset
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.dataset.FileDataset
import com.dimajix.spark.testing.LocalSparkSession


class CompareTargetTest extends AnyFlatSpec with Matchers with LocalSparkSession {
    "The CompareTarget" should "be parseable from YAML" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val spec =
            s"""
              |kind: compare
              |expected:
              |  kind: file
              |  location: $basedir/data/data_1.csv
              |actual:
              |  kind: file
              |  location: $basedir/data/data_1.csv
              |""".stripMargin
        val target = ObjectMapper.parse[TargetSpec](spec)
        target shouldBe a[CompareTargetSpec]
    }

    it should "work on same files" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/data_1.csv"), "csv")
        )

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set(
            ResourceIdentifier.ofFile(new Path(basedir, "data/data_1.csv"))
        ))
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        val result = target.execute(executor, Phase.VERIFY)
        result.target should be (target)
        result.phase should be (Phase.VERIFY)
        result.status should be (Status.SUCCESS)
        result.exception should be (None)
        result.numFailures should be (0)
        result.numSuccesses should be (0)
        result.numExceptions should be (0)
        result.children.size should be (0)

        session.shutdown()
    }

    it should "fail on non existing actual file" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/no_such_file"), "csv"),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/data_1.csv"), "csv")
        )

        target.requires(Phase.CREATE) should be (Set())
        target.requires(Phase.BUILD) should be (Set())
        target.requires(Phase.VERIFY) should be (Set(
            ResourceIdentifier.ofFile(new Path(basedir, "data/no_such_file")),
            ResourceIdentifier.ofFile(new Path(basedir, "data/data_1.csv"))
        ))
        target.requires(Phase.TRUNCATE) should be (Set())
        target.requires(Phase.DESTROY) should be (Set())

        val result = target.execute(executor, Phase.VERIFY)
        result.target should be (target)
        result.phase should be (Phase.VERIFY)
        result.status should be (Status.FAILED)
        result.exception.get shouldBe a[VerificationFailedException]
        result.numFailures should be (0)
        result.numSuccesses should be (0)
        result.numExceptions should be (1)
        result.children.size should be (0)

        session.shutdown()
    }

    it should "throw an exception on an non existing expected file" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/no_such_file"), "csv")
        )

        val result = target.execute(executor, Phase.VERIFY)
        result.target should be (target)
        result.phase should be (Phase.VERIFY)
        result.status should be (Status.FAILED)
        result.exception.get shouldBe a[AnalysisException]
        result.numFailures should be (0)
        result.numSuccesses should be (0)
        result.numExceptions should be (1)
        result.children.size should be (0)

        session.shutdown()
    }

    it should "work with a directory as expected" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/data_1.csv"), "csv"),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data"), "csv")
        )

        val result = target.execute(executor, Phase.VERIFY)
        result.target should be (target)
        result.phase should be (Phase.VERIFY)
        result.status should be (Status.SUCCESS)
        result.exception should be (None)
        result.numFailures should be (0)
        result.numSuccesses should be (0)
        result.numExceptions should be (0)
        result.children.size should be (0)

        session.shutdown()
    }

    it should "work with a directory as actual" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data"), "csv"),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/data_1.csv"), "csv")
        )

        val result = target.execute(executor, Phase.VERIFY)
        result.target should be (target)
        result.phase should be (Phase.VERIFY)
        result.status should be (Status.SUCCESS)
        result.exception should be (None)
        result.numFailures should be (0)
        result.numSuccesses should be (0)
        result.numExceptions should be (0)
        result.children.size should be (0)

        session.shutdown()
    }

    it should "work with a directory as expected and actual" in {
        val basedir = new Path(Resources.getResource(".").toURI)
        val session = Session.builder().withSparkSession(spark).build()
        val executor = session.execution
        val context = session.context

        val target = CompareTarget(
            Target.Properties(context),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/actual"), "csv"),
            FileDataset(Dataset.Properties(context), new Path(basedir, "data/expected"), "csv")
        )

        val result = target.execute(executor, Phase.VERIFY)
        result.target should be (target)
        result.phase should be (Phase.VERIFY)
        result.status should be (Status.SUCCESS)
        result.exception should be (None)
        result.numFailures should be (0)
        result.numSuccesses should be (0)
        result.numExceptions should be (0)
        result.children.size should be (0)

        session.shutdown()
    }
}
