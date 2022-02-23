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

import java.sql.Timestamp
import java.time.Instant
import java.util.Locale
import java.util.Properties

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.control.NonFatal

import org.slf4j.LoggerFactory
import slick.jdbc.JdbcProfile

import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.spec.connection.JdbcConnection
import com.dimajix.flowman.spec.metric.JdbcMetricRepository.Commit
import com.dimajix.flowman.spec.metric.JdbcMetricRepository.CommitLabel
import com.dimajix.flowman.spec.metric.JdbcMetricRepository.Measurement
import com.dimajix.flowman.spec.metric.JdbcMetricRepository.MetricLabel



private[metric] object JdbcMetricRepository {
    case class Commit(
        id:Long,
        ts:Timestamp
    )
    case class CommitLabel(
        commit_id:Long,
        name:String,
        value:String
    )
    case class Measurement(
        id:Long,
        commit_id:Long,
        name:String,
        ts:Timestamp,
        value:Double
    )
    case class MetricLabel(
        metric_id:Long,
        name:String,
        value:String
    )
}


private[metric] class JdbcMetricRepository(
    connection: JdbcConnection,
    val profile: JdbcProfile,
    commitTable: String = "flowman_metric_commits",
    commitLabelTable: String = "flowman_metric_commit_labels",
    metricTable: String = "flowman_metrics",
    metricLabelTable: String = "flowman_metric_labels"
) {
    private val logger = LoggerFactory.getLogger(getClass)

    import profile.api._

    private lazy val db = {
        val url = connection.url
        val driver = connection.driver
        val user = connection.username
        val password = connection.password
        val props = new Properties()
        connection.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        logger.debug(s"Connecting via JDBC to $url with driver $driver")
        val executor = slick.util.AsyncExecutor(
            name="Flowman.jdbc_metric_sink",
            minThreads = 20,
            maxThreads = 20,
            queueSize = 1000,
            maxConnections = 20)
        Database.forURL(url, driver=driver, user=user.orNull, password=password.orNull, prop=props, executor=executor)
    }

    class Commits(tag: Tag) extends Table[Commit](tag, commitTable) {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def ts = column[Timestamp]("ts")

        def * = (id, ts) <> (Commit.tupled, Commit.unapply)
    }
    class CommitLabels(tag: Tag) extends Table[CommitLabel](tag, commitLabelTable) {
        def commit_id = column[Long]("commit_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(64))

        def pk = primaryKey(commitLabelTable + "_pk", (commit_id, name))
        def commit = foreignKey(commitLabelTable + "_fk", commit_id, commits)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
        def idx = index(commitLabelTable + "_idx", (name, value), unique = false)

        def * = (commit_id, name, value) <> (CommitLabel.tupled, CommitLabel.unapply)
    }
    class Metrics(tag: Tag) extends Table[Measurement](tag, metricTable) {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def commit_id = column[Long]("commit_id")
        def name = column[String]("name", O.Length(64))
        def ts = column[Timestamp]("ts")
        def value = column[Double]("value")

        def commit = foreignKey(metricTable + "_fk", commit_id, commits)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, commit_id, name, ts, value) <> (Measurement.tupled, Measurement.unapply)
    }
    class MetricLabels(tag: Tag) extends Table[MetricLabel](tag, metricLabelTable) {
        def metric_id = column[Long]("metric_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(64))

        def pk = primaryKey(metricLabelTable + "_pk", (metric_id, name))
        def metric = foreignKey(metricLabelTable + "_fk", metric_id, metrics)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
        def idx = index(metricLabelTable + "_idx", (name, value), unique = false)

        def * = (metric_id, name, value) <> (MetricLabel.tupled, MetricLabel.unapply)
    }

    val commits = TableQuery[Commits]
    val commitLabels = TableQuery[CommitLabels]
    val metrics = TableQuery[Metrics]
    val metricLabels = TableQuery[MetricLabels]


    def create() : Unit = {
        import scala.concurrent.ExecutionContext.Implicits.global
        val tables = Seq(
            commits,
            commitLabels,
            metrics,
            metricLabels
        )

        try {
            val existing = db.run(profile.defaultTables)
            val query = existing.flatMap(v => {
                val names = v.map(mt => mt.name.name.toLowerCase(Locale.ROOT))
                val createIfNotExist = tables
                    .filter(table => !names.contains(table.baseTableRow.tableName.toLowerCase(Locale.ROOT)))
                    .map(_.schema.create)
                db.run(DBIO.sequence(createIfNotExist))
            })
            Await.result(query, Duration.Inf)
        }
        catch {
            case NonFatal(ex) => logger.error("Cannot connect to JDBC metric database to create tables", ex)
        }
    }

    def commit(metrics:Seq[GaugeMetric], labels:Map[String,String]) : Unit = {
        val ts = Timestamp.from(Instant.now())

        val cmQuery = (commits returning commits.map(_.id) into((jm, id) => jm.copy(id=id))) += Commit(0, ts)
        val commit = Await.result(db.run(cmQuery), Duration.Inf)
        val lbls = labels.map(l => CommitLabel(commit.id, l._1, l._2))
        val clQuery = commitLabels ++= lbls
        Await.result(db.run(clQuery), Duration.Inf)

        metrics.foreach { m =>
            val metrics = this.metrics
            val mtQuery = (metrics returning metrics.map(_.id) into((jm, id) => jm.copy(id=id))) += Measurement(0, commit.id, m.name, ts, m.value)
            val metric = Await.result(db.run(mtQuery), Duration.Inf)

            val lbls = m.labels.map(l => MetricLabel(metric.id, l._1, l._2))
            val mlQuery = metricLabels ++= lbls
            Await.result(db.run(mlQuery), Duration.Inf)
        }
    }
}
