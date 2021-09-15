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

import java.time.Instant

import scala.collection.immutable.ListMap

import org.apache.spark.storage.StorageLevel
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.Assertion
import com.dimajix.flowman.model.AssertionResult
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.MappingOutputIdentifier
import com.dimajix.flowman.model.NamespaceWrapper
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ProjectWrapper
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.TargetResult
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.model.TestWrapper
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
            .withSparkSession(spark)
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
                "test" -> TestWrapper(test),
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
                "test" -> TestWrapper(test),
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
            .withSparkSession(spark)
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
        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target", "default"))
        (target.name _).expects().atLeastOnce().returns("target")
        (target.instance _).expects().atLeastOnce().returns(TargetInstance("", "", "", Map()))
        (target.requires _).expects(*).atLeastOnce().returns(Set())
        (target.provides _).expects(*).atLeastOnce().returns(Set())
        (target.before _).expects().atLeastOnce().returns(Seq())
        (target.after _).expects().atLeastOnce().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (target.execute _).expects(*, Phase.CREATE).returns(TargetResult(target, Phase.CREATE, Status.SUCCESS, Instant.now()))
        (target.execute _).expects(*, Phase.BUILD).returns(TargetResult(target, Phase.BUILD, Status.SUCCESS, Instant.now()))
        (target.execute _).expects(*, Phase.VERIFY).returns(TargetResult(target, Phase.VERIFY, Status.SUCCESS, Instant.now()))
        (target.execute _).expects(*, Phase.DESTROY).returns(TargetResult(target, Phase.DESTROY, Status.SUCCESS, Instant.now()))

        (fixtureTemplate.instantiate _).expects(*).returns(fixture)
        (fixture.identifier _).expects().atLeastOnce().returns(TargetIdentifier("fixture", "default"))
        (fixture.name _).expects().atLeastOnce().returns("fixture")
        (fixture.instance _).expects().atLeastOnce().returns(TargetInstance("", "", "", Map()))
        (fixture.requires _).expects(*).atLeastOnce().returns(Set())
        (fixture.provides _).expects(*).atLeastOnce().returns(Set())
        (fixture.before _).expects().atLeastOnce().returns(Seq(TargetIdentifier("target", "default")))
        (fixture.after _).expects().atLeastOnce().returns(Seq())
        (fixture.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (fixture.execute _).expects(*, Phase.CREATE).returns(TargetResult(fixture, Phase.CREATE, Status.SUCCESS, Instant.now()))
        (fixture.execute _).expects(*, Phase.BUILD).returns(TargetResult(fixture, Phase.BUILD, Status.SUCCESS, Instant.now()))
        (fixture.execute _).expects(*, Phase.VERIFY).returns(TargetResult(fixture, Phase.VERIFY, Status.SUCCESS, Instant.now()))
        (fixture.execute _).expects(*, Phase.DESTROY).returns(TargetResult(fixture, Phase.DESTROY, Status.SUCCESS, Instant.now()))

        var assertionContext:Context = null
        (assertionTemplate.instantiate _).expects(*).onCall { ctx:Context =>
            assertionContext = ctx
            assertion
        }
        (assertion.name _).expects().atLeastOnce().returns("assertion")
        (assertion.description _).expects().atLeastOnce().returns(None)
        (assertion.context _).expects().onCall(() => assertionContext)
        (assertion.inputs _).expects().atLeastOnce().returns(Seq(MappingOutputIdentifier("map", "main", None)))
        (assertion.execute _).expects(*,*).returns(AssertionResult(assertion, Instant.now()))

        var overrideMappingContext:Context = null
        (overrideMappingTemplate.instantiate _).expects(*).onCall { ctx:Context =>
            overrideMappingContext = ctx
            overrideMapping
        }
        (overrideMapping.inputs _).expects().atLeastOnce().returns(Seq())
        (overrideMapping.outputs _).expects().atLeastOnce().returns(Seq("main"))
        (overrideMapping.identifier _).expects().atLeastOnce().returns(MappingIdentifier("map"))
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
            .withSparkSession(spark)
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
        (assertion.name _).expects().atLeastOnce().returns("assertion")
        (assertion.description _).expects().atLeastOnce().returns(None)

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
            .withSparkSession(spark)
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
        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target", "default"))
        (target.name _).expects().atLeastOnce().returns("target")
        (target.instance _).expects().atLeastOnce().returns(TargetInstance("default", "project", "target"))
        (target.requires _).expects(*).atLeastOnce().returns(Set())
        (target.provides _).expects(*).atLeastOnce().returns(Set())
        (target.before _).expects().atLeastOnce().returns(Seq())
        (target.after _).expects().atLeastOnce().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (target.execute _).expects(*, Phase.CREATE).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.BUILD).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.VERIFY).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.DESTROY).throws(new UnsupportedOperationException())

        (fixtureTemplate.instantiate _).expects(*).returns(fixture)
        (fixture.identifier _).expects().atLeastOnce().returns(TargetIdentifier("fixture", "default"))
        (fixture.name _).expects().atLeastOnce().returns("fixture")
        (fixture.instance _).expects().atLeastOnce().returns(TargetInstance("default", "project", "fixture"))
        (fixture.requires _).expects(*).atLeastOnce().returns(Set())
        (fixture.provides _).expects(*).atLeastOnce().returns(Set())
        (fixture.before _).expects().atLeastOnce().returns(Seq(TargetIdentifier("target", "default")))
        (fixture.after _).expects().atLeastOnce().returns(Seq())
        (fixture.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
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
        (assertion.name _).expects().atLeastOnce().returns("assertion")
        (assertion.description _).expects().atLeastOnce().returns(None)
        (assertion.inputs _).expects().atLeastOnce().returns(Seq(MappingOutputIdentifier("map", "main", None)))
        (assertion.execute _).expects(*,*).throws(new UnsupportedOperationException())

        var mappingContext:Context = null
        (mappingTemplate.instantiate _).expects(*).onCall { ctx:Context =>
            mappingContext = ctx
            mapping
        }
        (mapping.inputs _).expects().atLeastOnce().returns(Seq())
        (mapping.outputs _).expects().atLeastOnce().returns(Seq("main"))
        (mapping.identifier _).expects().atLeastOnce().returns(MappingIdentifier("map"))
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
            .withSparkSession(spark)
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
        (target.identifier _).expects().atLeastOnce().returns(TargetIdentifier("target", "default"))
        (target.name _).expects().atLeastOnce().returns("target")
        (target.instance _).expects().atLeastOnce().returns(TargetInstance("default", "project", "target"))
        (target.requires _).expects(*).atLeastOnce().returns(Set())
        (target.provides _).expects(*).atLeastOnce().returns(Set())
        (target.before _).expects().atLeastOnce().returns(Seq())
        (target.after _).expects().atLeastOnce().returns(Seq())
        (target.phases _).expects().atLeastOnce().returns(Set(Phase.CREATE, Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))
        (target.execute _).expects(*, Phase.CREATE).throws(new UnsupportedOperationException())
        (target.execute _).expects(*, Phase.DESTROY).throws(new UnsupportedOperationException())

        (fixtureTemplate.instantiate _).expects(*).returns(fixture)
        (fixture.identifier _).expects().atLeastOnce().returns(TargetIdentifier("fixture", "default"))
        (fixture.requires _).expects(*).atLeastOnce().returns(Set())
        (fixture.provides _).expects(*).atLeastOnce().returns(Set())
        (fixture.before _).expects().atLeastOnce().returns(Seq(TargetIdentifier("target", "default")))
        (fixture.after _).expects().atLeastOnce().returns(Seq())
        (fixture.phases _).expects().atLeastOnce().returns(Set(Phase.BUILD, Phase.VERIFY, Phase.TRUNCATE, Phase.DESTROY))

        runner.executeTest(test, keepGoing = false) should be (Status.FAILED)
    }

    it should "ignore exceptions in assertions if told so" in {
        val project = Project(
            name = "default"
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.getContext(project)

        val assertionTemplate1 = mock[Template[Assertion]]
        val assertion1 = mock[Assertion]
        val assertionTemplate2 = mock[Template[Assertion]]
        val assertion2 = mock[Assertion]
        val test = Test(
            Test.Properties(context),
            assertions = Map(
                "assert1" -> assertionTemplate1,
                "assert2" -> assertionTemplate2
            )
        )

        val runner = session.runner

        (assertionTemplate1.instantiate _).expects(*).returns(assertion1)
        (assertion1.context _).expects().returns(context)
        (assertion1.name _).expects().atLeastOnce().returns("assertion1")
        (assertion1.description _).expects().atLeastOnce().returns(None)
        (assertion1.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion1.execute _).expects(*,*).throws(new UnsupportedOperationException())
        (assertionTemplate2.instantiate _).expects(*).returns(assertion2)
        (assertion2.context _).expects().returns(context)
        (assertion2.description _).expects().atLeastOnce().returns(None)
        (assertion2.name _).expects().atLeastOnce().returns("assertion2")
        (assertion2.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion2.execute _).expects(*,*).returns(AssertionResult(assertion2, Instant.now()))

        runner.executeTest(test, keepGoing = true) should be (Status.FAILED)
    }

    it should "stop on the first exceptions in assertions if told so" in {
        val project = Project(
            name = "default"
        )
        val session = Session.builder()
            .withSparkSession(spark)
            .build()
        val context = session.getContext(project)

        val assertionTemplate1 = mock[Template[Assertion]]
        val assertion1 = mock[Assertion]
        val assertionTemplate2 = mock[Template[Assertion]]
        val assertion2 = mock[Assertion]
        val test = Test(
            Test.Properties(context),
            assertions = ListMap(
                "assert1" -> assertionTemplate1,
                "assert2" -> assertionTemplate2
            )
        )

        val runner = session.runner

        (assertionTemplate1.instantiate _).expects(*).returns(assertion1)
        (assertion1.context _).expects().returns(context)
        (assertion1.name _).expects().atLeastOnce().returns("assertion1")
        (assertion1.description _).expects().atLeastOnce().returns(None)
        (assertion1.inputs _).expects().atLeastOnce().returns(Seq())
        (assertion1.execute _).expects(*,*).throws(new UnsupportedOperationException())
        (assertionTemplate2.instantiate _).expects(*).returns(assertion2)
        (assertion2.name _).expects().atLeastOnce().returns("assertion2")
        (assertion2.description _).expects().atLeastOnce().returns(None)
        (assertion2.inputs _).expects().returns(Seq())

        runner.executeTest(test, keepGoing = false) should be (Status.FAILED)
    }
}
