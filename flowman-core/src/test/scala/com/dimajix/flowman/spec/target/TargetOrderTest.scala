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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Lifecycle
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.spec.ResourceIdentifier


case class DummyTarget(
    override val context: Context,
    override val name: String,
    providedResources : Set[ResourceIdentifier],
    requiredResources : Set[ResourceIdentifier]
) extends BaseTarget {
    protected override def instanceProperties: Target.Properties = Target.Properties(context, name)

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


class TargetOrderTest extends FlatSpec with Matchers {
    "Ordering" should "work with simple resources" in {
        val session = Session.builder().build()
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

        orderTargets(Seq(t1,t2,t3,t4), Phase.BUILD).map(_.name) should be (Seq("t1","t2","t3","t4"))
    }

    it should "work with partitions" in {
        val session = Session.builder().build()
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

        orderTargets(Seq(t1,t2,t3,t4), Phase.BUILD).map(_.name) should be (Seq("t1","t2","t3","t4"))
    }

    it should "work with wildcards" in {
        val session = Session.builder().build()
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

        orderTargets(Seq(t1,t2,t3,t4), Phase.BUILD).map(_.name) should be (Seq("t1","t2","t3","t4"))
    }
}
