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

import org.apache.hadoop.fs.Path
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier


case class DummyTarget(
    override val context: Context,
    override val name: String,
    providedResources : Set[ResourceIdentifier] = Set(),
    requiredResources : Set[ResourceIdentifier] = Set(),
    bfore : Seq[TargetIdentifier] = Seq(),
    aftr : Seq[TargetIdentifier] = Seq()
) extends BaseTarget {
    protected override def instanceProperties: Target.Properties =
        Target.Properties(context, name).copy(before=bfore, after=aftr)

    /**
     * Returns all phases which are implemented by this target in the execute method
     *
     * @return
     */
    override def phases : Set[Phase] = Lifecycle.ALL.toSet

    /**
     * Returns a list of physical resources required by this target
     * @return
     */
    override def requires(phase: Phase) : Set[ResourceIdentifier] = requiredResources

    /**
     * Returns a list of physical resources produced by this target
     *
     * @return
     */
    override def provides(phase: Phase): Set[ResourceIdentifier] = providedResources
}


class DependencySchedulerTest extends AnyFlatSpec with Matchers {
    private def sort(targets:Seq[Target], phase:Phase) : Seq[Target] = {
        val scheduler = new DependencyScheduler
        Scheduler.sort(scheduler, targets, phase, (_:Target) => true)
    }
    private def sort(targets:Seq[Target], phase:Phase, filter:(Target) => Boolean) : Seq[Target] = {
        val scheduler = new DependencyScheduler
        Scheduler.sort(scheduler, targets, phase, filter)
    }

    "The DependencyScheduler" should "work with simple resources" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val t1 = DummyTarget(context, "t1",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t1")),
                ResourceIdentifier.ofFile(new Path("/some/t1/xyz"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/nonexisting/file"))
            )
        )
        val t2 = DummyTarget(context, "t2",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t2"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t1/xyz")),
                ResourceIdentifier.ofFile(new Path("/some/nonexisting/file"))
            )
        )
        val t3 = DummyTarget(context, "t3",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t3"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t1")),
                ResourceIdentifier.ofFile(new Path("/some/t2")),
                ResourceIdentifier.ofFile(new Path("/some/nonexisting/file"))
            )
        )
        val t4 = DummyTarget(context, "t4",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t4"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t1/xyz")),
                ResourceIdentifier.ofFile(new Path("/some/t2")),
                ResourceIdentifier.ofFile(new Path("/some/t3"))
            )
        )

        sort(Seq(t1,t2,t3,t4), Phase.BUILD).map(_.name) should be (Seq("t1","t2","t3","t4"))
        sort(Seq(t1,t2,t3,t4), Phase.DESTROY).map(_.name) should be (Seq("t4","t3","t2","t1"))

        session.shutdown()
    }

    it should "work with partitions" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val t1 = DummyTarget(context, "t1",
            Set(
                ResourceIdentifier.ofHivePartition("/some/t1", None, Map("p1" -> "xyz", "p2" -> "abc")),
                ResourceIdentifier.ofFile(new Path("/some/t1/xyz"))
            ),
            Set()
        )
        val t2 = DummyTarget(context, "t2",
            Set(
                ResourceIdentifier.ofHivePartition("/some/t2", None, Map("p1" -> "1234"))
            ),
            Set(
                ResourceIdentifier.ofHivePartition("/some/t1", None, Map("p1" -> "xyz"))
            )
        )
        val t3 = DummyTarget(context, "t3",
            Set(
                ResourceIdentifier.ofHivePartition("/some/t3", None, Map("p1" -> "1234"))
            ),
            Set(
                ResourceIdentifier.ofHivePartition("/some/t2", None, Map()),
                ResourceIdentifier.ofHivePartition("/some/t1", None, Map("p2" -> "abc"))
            )
        )
        val t4 = DummyTarget(context, "t4",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t4"))
            ),
            Set(
                ResourceIdentifier.ofHivePartition("/some/t2", None, Map()),
                ResourceIdentifier.ofHivePartition("/some/t3", None, Map())
            )
        )

        sort(Seq(t1,t2,t3,t4), Phase.BUILD).map(_.name) should be (Seq("t1","t2","t3","t4"))
        sort(Seq(t1,t2,t3,t4), Phase.DESTROY).map(_.name) should be (Seq("t4","t3","t2","t1"))

        session.shutdown()
    }

    it should "work with wildcards" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val t1 = DummyTarget(context, "t1",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t1")),
                ResourceIdentifier.ofFile(new Path("/some/t1/p1/xyz"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/*/nonexisting/file"))
            )
        )
        val t2 = DummyTarget(context, "t2",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t2/abc"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t1/*/xyz")),
                ResourceIdentifier.ofFile(new Path("/some/nonexisting/file"))
            )
        )
        val t3 = DummyTarget(context, "t3",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t3/xyz"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t1/*/xyz")),
                ResourceIdentifier.ofFile(new Path("/some/t2/*")),
                ResourceIdentifier.ofFile(new Path("/some/nonexisting/file"))
            )
        )
        val t4 = DummyTarget(context, "t4",
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t4"))
            ),
            Set(
                ResourceIdentifier.ofFile(new Path("/some/t3/*")),
                ResourceIdentifier.ofFile(new Path("/some/t1")),
                ResourceIdentifier.ofFile(new Path("/some/t2/*"))
            )
        )

        sort(Seq(t1,t2,t3,t4), Phase.BUILD).map(_.name) should be (Seq("t1","t2","t3","t4"))
        sort(Seq(t1,t2,t3,t4), Phase.DESTROY).map(_.name) should be (Seq("t4","t3","t2","t1"))

        session.shutdown()
    }

    it should "work with Windows paths" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val t1 = DummyTarget(context, "t1",
            Set(ResourceIdentifier.ofFile(new Path("C:/Temp/1572861822921-0/topic=publish.Card.test.dev/processing_date=2019-03-20"))),
            Set()
        )
        val t2 = DummyTarget(context, "t2",
            Set(),
            Set(
                ResourceIdentifier.ofFile(new Path("C:/Temp/1572861822921-0/topic=publish.Card.*.dev/processing_date=2019-03-20")),
                ResourceIdentifier.ofFile(new Path("C:/Temp/1572861822921-0/topic=publish.Card.*.dev"))
            )
        )

        sort(Seq(t1,t2), Phase.BUILD).map(_.name) should be (Seq("t1","t2"))
        sort(Seq(t2,t1), Phase.BUILD).map(_.name) should be (Seq("t1","t2"))

        session.shutdown()
    }

    it should "work with before and after" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val t1 = DummyTarget(context, "t1",
            bfore = Seq(TargetIdentifier("t2"))
        )
        val t2 = DummyTarget(context, "t2",
            aftr = Seq(TargetIdentifier("t3"))
        )
        val t3 = DummyTarget(context, "t3",
            aftr = Seq(TargetIdentifier("t4"))
        )
        val t4 = DummyTarget(context, "t4")

        val order1 = sort(Seq(t1,t2,t3,t4), Phase.BUILD).map(_.name).zipWithIndex.toMap
        (order1("t1") < order1("t2")) should be (true)
        (order1("t2") > order1("t3")) should be (true)
        (order1("t3") > order1("t4")) should be (true)

        val order2 = sort(Seq(t4,t3,t2,t1), Phase.BUILD).map(_.name).zipWithIndex.toMap
        (order2("t1") < order2("t2")) should be (true)
        (order2("t2") > order2("t3")) should be (true)
        (order2("t3") > order2("t4")) should be (true)

        val order3 = sort(Seq(t4,t3,t2,t1), Phase.DESTROY).map(_.name).zipWithIndex.toMap
        (order3("t1") > order3("t2")) should be (true)
        (order3("t2") < order3("t3")) should be (true)
        (order3("t3") < order3("t4")) should be (true)

        session.shutdown()
    }

    it should "work with filters" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val t1 = DummyTarget(context, "t1",
            bfore = Seq(TargetIdentifier("t2"))
        )
        val t2 = DummyTarget(context, "t2",
            bfore = Seq(TargetIdentifier("t3"))
        )
        val t3 = DummyTarget(context, "t3")

        def filter(target:Target) : Boolean = (target.name == "t1" || target.name == "t3")
        sort(Seq(t1,t2,t3), Phase.BUILD, filter).map(_.name) should be (Seq("t1", "t3"))
        sort(Seq(t3,t2,t1), Phase.BUILD, filter).map(_.name) should be (Seq("t1", "t3"))
        sort(Seq(t1,t2,t3), Phase.DESTROY, filter).map(_.name) should be (Seq("t3", "t1"))

        session.shutdown()
    }

    it should "error on cyclic dependencies" in {
        val session = Session.builder()
            .disableSpark()
            .build()
        val context = session.context

        val t1 = DummyTarget(context, "t1",
            bfore = Seq(TargetIdentifier("t2"))
        )
        val t2 = DummyTarget(context, "t2",
            bfore = Seq(TargetIdentifier("t1"))
        )

        a[RuntimeException] should be thrownBy (sort(Seq(t1,t2), Phase.BUILD))

        session.shutdown()
    }
}
