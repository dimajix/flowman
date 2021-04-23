/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.testing

import com.dimajix.flowman.model.Project
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RunnerTest extends AnyFlatSpec with Matchers {
    "The Runner" should "apply all configs" in {
        val project = Project(name="project")
        val runner = Runner.builder()
            .withProject(project)
            .withConfig("spark.lala", "spark_lala")
            .withConfig("flowman.lolo", "flowman_lolo")
            .withConfig("other.abc", "other_abc")
            .build()

        runner.session.sparkConf.get("spark.lala") should be ("spark_lala")
        runner.session.sparkConf.contains("flowman.lolo") should be (false)
        runner.session.sparkConf.get("other.abc") should be ("other_abc")

        runner.session.flowmanConf.get("flowman.lolo") should be ("flowman_lolo")
        runner.session.flowmanConf.contains("spark.lala") should be (false)
        runner.session.flowmanConf.contains("other.abc") should be (false)

        runner.session.config.get("spark.lala") should be ("spark_lala")
        runner.session.config.get("flowman.lolo") should be ("flowman_lolo")
        runner.session.config.get("other.abc") should be ("other_abc")
    }

    it should "not work without a project" in {
        an[IllegalArgumentException] should be thrownBy (Runner.builder().build())
    }

    it should "work with a project" in {
        val project = Project(name="project")
        val runner = Runner.builder()
            .withProject(project)
            .build()

        runner should not be (null)
        runner.session should not be (null)
        runner.session.project should be (Some(project))
        runner.session.context.project should be (None)
    }
}
