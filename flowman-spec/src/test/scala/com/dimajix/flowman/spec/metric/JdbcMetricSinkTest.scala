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

package com.dimajix.flowman.spec.metric

import java.nio.file.Files
import java.nio.file.Path

import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.flowman.execution.RootContext
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.FixedGaugeMetric
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSelection
import com.dimajix.flowman.metric.MetricSystem
import com.dimajix.flowman.metric.Selector
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.ConnectionReference
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Prototype
import com.dimajix.flowman.spec.ObjectMapper
import com.dimajix.flowman.spec.connection.JdbcConnection


class JdbcMetricSinkTest extends AnyFlatSpec with Matchers with MockFactory with BeforeAndAfter {
    var tempDir:Path = _

    before {
        tempDir = Files.createTempDirectory("jdbc_metric_test")
    }
    after {
        tempDir.toFile.listFiles().foreach(_.delete())
        tempDir.toFile.delete()
    }

    "The JdbcMetricSink" should "be parsable" in {
        val spec =
            """
              |kind: jdbc
              |connection: metrics
              |tablePrefix: prefix_
            """.stripMargin

        val monitor = ObjectMapper.parse[MetricSinkSpec](spec)
        monitor shouldBe a[JdbcMetricSinkSpec]
    }

    it should "be parsable with an embedded connection" in {
        val spec =
            """
              |kind: jdbc
              |connection:
              |  kind: jdbc
              |  url: some_url
            """.stripMargin

        val monitor = ObjectMapper.parse[MetricSinkSpec](spec)
        monitor shouldBe a[JdbcMetricSinkSpec]
    }

    it should "work" in {
        val db = tempDir.resolve("mydb")
        val project = Project("prj1")
        val context = RootContext.builder().build().getProjectContext(project)

        val connection = JdbcConnection(
            Connection.Properties(context),
            url = "jdbc:derby:" + db + ";create=true",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )
        val connectionPrototype = mock[Prototype[Connection]]
        (connectionPrototype.instantiate _).expects(context).returns(connection)

        val sink = new JdbcMetricSink(
            ConnectionReference.apply(context, connectionPrototype),
            Map("project" -> s"${project.name}")
        )

        val metricSystem = new MetricSystem
        val metricBoard = MetricBoard(context,
            Map("board_label" -> "v1"),
            Seq(MetricSelection(selector=Selector(".*"), labels=Map("target" -> "$target", "status" -> "$status")))
        )

        metricSystem.addMetric(FixedGaugeMetric("metric1", labels=Map("target" -> "p1", "metric_label" -> "v2"), 23.0))

        sink.addBoard(metricBoard, metricSystem)
        sink.commit(metricBoard, Status.SUCCESS)
        sink.commit(metricBoard, Status.SUCCESS)
    }

    it should "throw on non-existing database" in {
        val db = tempDir.resolve("mydb2")
        val context = RootContext.builder().build()

        val connection = JdbcConnection(
            Connection.Properties(context),
            url = "jdbc:derby:" + db + ";create=false",
            driver = "org.apache.derby.jdbc.EmbeddedDriver"
        )
        val connectionPrototype = mock[Prototype[Connection]]
        (connectionPrototype.instantiate _).expects(context).returns(connection)

        val sink = new JdbcMetricSink(
            ConnectionReference.apply(context, connectionPrototype)
        )

        val metricSystem = new MetricSystem
        val metricBoard = MetricBoard(context,
            Map("board_label" -> "v1"),
            Seq(MetricSelection(selector=Selector(".*"), labels=Map("target" -> "$target", "status" -> "$status")))
        )

        sink.addBoard(metricBoard, metricSystem)
        an[Exception] should be thrownBy(sink.commit(metricBoard, Status.SUCCESS))
        an[Exception] should be thrownBy(sink.commit(metricBoard, Status.SUCCESS))
    }
}
