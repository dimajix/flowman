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

package com.dimajix.flowman.execution

import org.apache.spark.storage.StorageLevel
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.NamespaceWrapper
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ProjectWrapper
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.Test
import com.dimajix.spark.testing.LocalSparkSession


class RunnerTestTest extends AnyFlatSpec with MockFactory with Matchers with LocalSparkSession {
    "The Runner for Tests" should "register override mappings and relations in tests" in {
        val project = Project(
            name = "default",
            environment = Map(
                "project_env" -> "project",
                "project_env_to_overwrite" -> "project"
            )
        )
        val session = Session.builder()
            .withEnvironment("global_env", "global")
            .withEnvironment("global_env_to_overwrite", "global")
            .withProject(project)
            .build()
        val context = session.getContext(project)
        val test = Test(
            Test.Properties(context),
            environment = Map(
                "test_env" -> "test_env",
                "project_env_to_overwrite" -> "test",
                "global_env_to_overwrite" -> "test"
            )
        )

        val runner = session.runner

        runner.withTestContext(test, dryRun=false) { context =>
            context.environment.toMap should be(Map(
                "global_env" -> "global",
                "project_env" -> "project",
                "project_env_to_overwrite" -> "test",
                "test_env" -> "test_env",
                "global_env_to_overwrite" -> "global",
                "force" -> false,
                "dryRun" -> false,
                "project" -> ProjectWrapper(project),
                "namespace" -> NamespaceWrapper(None)
            ))
        }
        runner.withEnvironment(test, dryRun=false) { environment =>
            environment.toMap should be(Map(
                "global_env" -> "global",
                "project_env" -> "project",
                "project_env_to_overwrite" -> "test",
                "test_env" -> "test_env",
                "global_env_to_overwrite" -> "global",
                "force" -> false,
                "dryRun" -> false,
                "project" -> ProjectWrapper(project),
                "namespace" -> NamespaceWrapper(None)
            ))
        }
    }

    it should "correctly build targets and fixtures and check assertions" in {
        val targetTemplate = mock[Template[Target]]
        val target = mock[Target]
        val relationTemplate = mock[Template[Relation]]
        val mappingTemplate = mock[Template[Mapping]]
        val project = Project(
            name = "default",
            targets = Map(
                "target" -> targetTemplate
            ),
            relations = Map(
                "rel" -> relationTemplate
            ),
            mappings = Map(
                "map" -> mappingTemplate
            )
        )
        val session = Session.builder()
            .build()
        val context = session.getContext(project)

        val fixtureTemplate = mock[Template[Target]]
        val fixture = mock[Target]
        val assertionTemplate = mock[Template[Assertion]]
        val assertion = mock[Assertion]
        val overrideRelationTemplate = mock[Template[Relation]]
        val overrideMappingTemplate = mock[Template[Mapping]]
        val overrideMapping = mock[Mapping]
        val test = Test(
            Test.Properties(context),
            targets = Seq(TargetIdentifier("target")),
            fixtures = Map(
                "fixture" -> fixtureTemplate
            ),
            overrideRelations = Map(
                "rel" -> overrideRelationTemplate
            ),
            overrideMappings = Map(
                "map" -> overrideMappingTemplate
            ),
            assertions = Map(
                "assert" -> assertionTemplate
            )
        )

        val runner = session.runner

        (targetTemplate.instantiate _).expects(*).returns(target)
        (target.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier("target", "default"))
        (target.requires _).expects(*).anyNumberOfTimes().returns(Set())
        (target.provides _).expects(*).anyNumberOfTimes().returns(Set())
        (target.before _).expects().anyNumberOfTimes().returns(Seq())
        (target.after _).expects().anyNumberOfTimes().returns(Seq())
        (target.phases _).expects().anyNumberOfTimes().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (target.execute _).expects(*, Phase.CREATE).returns(Unit)
        (target.execute _).expects(*, Phase.BUILD).returns(Unit)
        (target.execute _).expects(*, Phase.VERIFY).returns(Unit)
        (target.execute _).expects(*, Phase.DESTROY).returns(Unit)

        (fixtureTemplate.instantiate _).expects(*).returns(fixture)
        (fixture.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier("fixture", "default"))
        (fixture.requires _).expects(*).anyNumberOfTimes().returns(Set())
        (fixture.provides _).expects(*).anyNumberOfTimes().returns(Set())
        (fixture.before _).expects().anyNumberOfTimes().returns(Seq())
        (fixture.after _).expects().anyNumberOfTimes().returns(Seq())
        (fixture.phases _).expects().anyNumberOfTimes().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (fixture.execute _).expects(*, Phase.CREATE).returns(Unit)
        (fixture.execute _).expects(*, Phase.BUILD).returns(Unit)
        (fixture.execute _).expects(*, Phase.VERIFY).returns(Unit)
        (fixture.execute _).expects(*, Phase.DESTROY).returns(Unit)

        var assertionContext:Context = null
        (assertionTemplate.instantiate _).expects(*).onCall { ctx:Context =>
            assertionContext = ctx
            assertion
        }
        (assertion.description _).expects().anyNumberOfTimes().returns(None)
        (assertion.context _).expects().onCall(() => assertionContext)
        (assertion.inputs _).expects().returns(Seq(MappingOutputIdentifier("map", "main", None)))
        (assertion.execute _).expects(*,*).returns(Seq())

        var overrideMappingContext:Context = null
        (overrideMappingTemplate.instantiate _).expects(*).onCall { ctx:Context =>
            overrideMappingContext = ctx
            overrideMapping
        }
        (overrideMapping.inputs _).expects().anyNumberOfTimes().returns(Seq())
        (overrideMapping.outputs _).expects().anyNumberOfTimes().returns(Seq("main"))
        (overrideMapping.identifier _).expects().anyNumberOfTimes().returns(MappingIdentifier("map"))
        (overrideMapping.context _).expects().onCall(() => overrideMappingContext)
        (overrideMapping.broadcast _).expects().returns(false)
        (overrideMapping.checkpoint _).expects().returns(false)
        (overrideMapping.cache _).expects().returns(StorageLevel.NONE)
        (overrideMapping.execute _).expects(*,*).returns(Map("main" -> spark.emptyDataFrame))

        runner.executeTest(test) should be (Status.SUCCESS)
    }

    it should "not execute assertions in dry run mode" in {
        val project = Project(
            name = "default"
        )
        val session = Session.builder()
            .build()
        val context = session.getContext(project)

        val assertionTemplate = mock[Template[Assertion]]
        val assertion = mock[Assertion]
        val test = Test(
            Test.Properties(context),
            assertions = Map(
                "assert" -> assertionTemplate
            )
        )

        val runner = session.runner

        (assertionTemplate.instantiate _).expects(*).returns(assertion)
        (assertion.description _).expects().anyNumberOfTimes().returns(None)

        runner.executeTest(test, dryRun = true) should be (Status.SUCCESS)
    }

    it should "ignore errors if told so" in {
        val targetTemplate = mock[Template[Target]]
        val target = mock[Target]
        val mappingTemplate = mock[Template[Mapping]]
        val mapping = mock[Mapping]
        val project = Project(
            name = "default",
            targets = Map(
                "target" -> targetTemplate
            ),
            mappings = Map(
                "map" -> mappingTemplate
            )
        )
        val session = Session.builder()
            .build()
        val context = session.getContext(project)

        val fixtureTemplate = mock[Template[Target]]
        val fixture = mock[Target]
        val assertionTemplate = mock[Template[Assertion]]
        val assertion = mock[Assertion]
        val test = Test(
            Test.Properties(context),
            targets = Seq(TargetIdentifier("target")),
            fixtures = Map(
                "fixture" -> fixtureTemplate
            ),
            assertions = Map(
                "assert" -> assertionTemplate
            )
        )

        val runner = session.runner

        (targetTemplate.instantiate _).expects(*).returns(target)
        (target.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier("target", "default"))
        (target.requires _).expects(*).anyNumberOfTimes().returns(Set())
        (target.provides _).expects(*).anyNumberOfTimes().returns(Set())
        (target.before _).expects().anyNumberOfTimes().returns(Seq())
        (target.after _).expects().anyNumberOfTimes().returns(Seq())
        (target.phases _).expects().anyNumberOfTimes().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (target.execute _).expects(*, Phase.CREATE).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.BUILD).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.VERIFY).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.DESTROY).throws(new UnsupportedOperationException())

        (fixtureTemplate.instantiate _).expects(*).returns(fixture)
        (fixture.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier("fixture", "default"))
        (fixture.requires _).expects(*).anyNumberOfTimes().returns(Set())
        (fixture.provides _).expects(*).anyNumberOfTimes().returns(Set())
        (fixture.before _).expects().anyNumberOfTimes().returns(Seq())
        (fixture.after _).expects().anyNumberOfTimes().returns(Seq())
        (fixture.phases _).expects().anyNumberOfTimes().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (fixture.execute _).expects(*, Phase.CREATE).throws(new UnsupportedOperationException())
        (fixture.execute _).expects(*, Phase.BUILD).throws(new UnsupportedOperationException())
        (fixture.execute _).expects(*, Phase.VERIFY).throws(new UnsupportedOperationException())
        (fixture.execute _).expects(*, Phase.DESTROY).throws(new UnsupportedOperationException())

        var assertionContext:Context = null
        (assertionTemplate.instantiate _).expects(*).onCall { ctx:Context =>
            assertionContext = ctx
            assertion
        }
        (assertion.context _).expects().onCall(() => assertionContext)
        (assertion.description _).expects().anyNumberOfTimes().returns(None)
        (assertion.inputs _).expects().returns(Seq(MappingOutputIdentifier("map", "main", None)))
        (assertion.execute _).expects(*,*).throws(new UnsupportedOperationException())

        var mappingContext:Context = null
        (mappingTemplate.instantiate _).expects(*).onCall { ctx:Context =>
            mappingContext = ctx
            mapping
        }
        (mapping.inputs _).expects().anyNumberOfTimes().returns(Seq())
        (mapping.outputs _).expects().anyNumberOfTimes().returns(Seq("main"))
        (mapping.identifier _).expects().anyNumberOfTimes().returns(MappingIdentifier("map"))
        (mapping.context _).expects().onCall(() => mappingContext)
        (mapping.broadcast _).expects().returns(false)
        (mapping.checkpoint _).expects().returns(false)
        (mapping.cache _).expects().returns(StorageLevel.NONE)
        (mapping.execute _).expects(*,*).returns(Map("main" -> spark.emptyDataFrame))

        runner.executeTest(test, keepGoing = true) should be (Status.FAILED)
    }

    it should "stop processing on the first exception" in {
        val targetTemplate = mock[Template[Target]]
        val target = mock[Target]
        val project = Project(
            name = "default",
            targets = Map(
                "target" -> targetTemplate
            )
        )
        val session = Session.builder()
            .build()
        val context = session.getContext(project)

        val fixtureTemplate = mock[Template[Target]]
        val fixture = mock[Target]
        val assertionTemplate = mock[Template[Assertion]]
        val test = Test(
            Test.Properties(context),
            targets = Seq(TargetIdentifier("target")),
            fixtures = Map(
                "fixture" -> fixtureTemplate
            ),
            assertions = Map(
                "assert" -> assertionTemplate
            )
        )

        val runner = session.runner

        (targetTemplate.instantiate _).expects(*).returns(target)
        (target.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier("target", "default"))
        (target.requires _).expects(*).anyNumberOfTimes().returns(Set())
        (target.provides _).expects(*).anyNumberOfTimes().returns(Set())
        (target.before _).expects().anyNumberOfTimes().returns(Seq())
        (target.after _).expects().anyNumberOfTimes().returns(Seq())
        (target.phases _).expects().anyNumberOfTimes().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (target.execute _).expects(*, Phase.CREATE).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.DESTROY).throws(new UnsupportedOperationException())

        (fixtureTemplate.instantiate _).expects(*).returns(fixture)
        (fixture.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier("fixture", "default"))
        (fixture.requires _).expects(*).anyNumberOfTimes().returns(Set())
        (fixture.provides _).expects(*).anyNumberOfTimes().returns(Set())
        (fixture.before _).expects().anyNumberOfTimes().returns(Seq())
        (fixture.after _).expects().anyNumberOfTimes().returns(Seq())
        (fixture.phases _).expects().anyNumberOfTimes().returns(Set(Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))

        runner.executeTest(test, keepGoing = false) should be (Status.FAILED)
    }
}
