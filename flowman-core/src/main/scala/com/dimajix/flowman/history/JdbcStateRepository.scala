/*
 * Copyright 2018-2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.history

import java.sql.Timestamp
import java.time.ZoneId
import java.util.Locale
import java.util.Properties

import scala.language.higherKinds
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import org.slf4j.LoggerFactory
import slick.jdbc.JdbcProfile

import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status


private[history] object JdbcStateRepository {
    private val logger = LoggerFactory.getLogger(classOf[JdbcStateRepository])

    case class JobRun(
        id:Long,
        namespace: String,
        project:String,
        bundle:String,
        phase:String,
        args_hash:String,
        start_ts:Timestamp,
        end_ts:Timestamp,
        status:String
    ) extends JobToken

    case class JobArgument(
        job_id:Long,
        name:String,
        value:String
    )

    case class TargetRun(
        id:Long,
        job_id:Option[Long],
        namespace: String,
        project:String,
        target:String,
        phase:String,
        partitions_hash:String,
        start_ts:Timestamp,
        end_ts:Timestamp,
        status:String
    ) extends TargetToken

    case class TargetPartition(
        target_id:Long,
        name:String,
        value:String
    )
}


private[history] class JdbcStateRepository(connection: JdbcStateStore.Connection, val profile:JdbcProfile) {
    import profile.api._

    import JdbcStateRepository._

    private lazy val db = {
        val url = connection.url
        val user = connection.user
        val password = connection.password
        val driver = connection.driver
        val props = new Properties()
        connection.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        logger.info(s"Connecting via JDBC to $url with driver $driver")
        Database.forURL(url, user=user, password=password, prop=props, driver=driver)
    }

    val jobRuns = TableQuery[JobRuns]
    val jobArgs = TableQuery[JobArguments]
    val targetRuns = TableQuery[TargetRuns]
    val targetPartitions = TableQuery[TargetPartitions]

    class JobRuns(tag:Tag) extends Table[JobRun](tag, "JOB_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def namespace = column[String]("namespace")
        def project = column[String]("project")
        def job = column[String]("job")
        def phase = column[String]("phase")
        def args_hash = column[String]("args_hash")
        def start_ts = column[Timestamp]("start_ts")
        def end_ts = column[Timestamp]("end_ts")
        def status = column[String]("status")

        def idx = index("JOB_RUN_IDX", (namespace, project, job, phase, args_hash, status), unique = false)

        def * = (id, namespace, project, job, phase, args_hash, start_ts, end_ts, status) <> (JobRun.tupled, JobRun.unapply)
    }

    class JobArguments(tag: Tag) extends Table[JobArgument](tag, "JOB_ARGUMENT") {
        def job_id = column[Long]("job_id")
        def name = column[String]("name")
        def value = column[String]("value")

        def pk = primaryKey("JOB_ARGUMENT_PK", (job_id, name))
        def batch = foreignKey("JOB_ARGUMENT_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (job_id, name, value) <> (JobArgument.tupled, JobArgument.unapply)
    }

    class TargetRuns(tag: Tag) extends Table[TargetRun](tag, "TARGET_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def batch_id = column[Option[Long]]("batch_id")
        def namespace = column[String]("namespace")
        def project = column[String]("project")
        def target = column[String]("target")
        def phase = column[String]("phase")
        def partitions_hash = column[String]("partitions_hash")
        def start_ts = column[Timestamp]("start_ts")
        def end_ts = column[Timestamp]("end_ts")
        def status = column[String]("status")

        def idx = index("TARGET_RUN_IDX", (namespace, project, target, phase, partitions_hash, status), unique = false)
        def batch = foreignKey("TARGET_RUN_BATCH_RUN_FK", batch_id, jobRuns)(_.id.?, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, batch_id, namespace, project, target, phase, partitions_hash, start_ts, end_ts, status) <> (TargetRun.tupled, TargetRun.unapply)
    }

    class TargetPartitions(tag: Tag) extends Table[TargetPartition](tag, "TARGET_PARTITION") {
        def target_id = column[Long]("target_id")
        def name = column[String]("name")
        def value = column[String]("value")

        def pk = primaryKey("TARGET_PARTITION_PK", (target_id, name))
        def target = foreignKey("TARGET_PARTITION_TARGET_RUN_FK", target_id, targetRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (target_id, name, value) <> (TargetPartition.tupled, TargetPartition.unapply)
    }

    implicit class QueryWrapper[E,U,C[_]] (query:Query[E, U, C]) {
        def optionalFilter[T](value:Option[T])(f:(E,T) => Rep[Boolean]) : Query[E,U,C] = {
            if (value.nonEmpty)
                query.filter(v => f(v,value.get))
            else
                query
        }
        def optionalFilter2[T](value:Option[T])(f:(E,T) => Rep[Option[Boolean]]) : Query[E,U,C] = {
            if (value.nonEmpty)
                query.filter(v => f(v,value.get))
            else
                query
        }
    }

    def create() : Unit = {
        import scala.concurrent.ExecutionContext.Implicits.global
        val tables = Seq(jobRuns, jobArgs, targetRuns, targetPartitions)

        val existing = db.run(profile.defaultTables)
        val query = existing.flatMap( v => {
            val names = v.map(mt => mt.name.name.toLowerCase(Locale.ROOT))
            val createIfNotExist = tables
                .filter(table => !names.contains(table.baseTableRow.tableName.toLowerCase(Locale.ROOT)))
                .map(_.schema.create)
            db.run(DBIO.sequence(createIfNotExist))
        })
        Await.result(query, Duration.Inf)
    }

    def getJobState(run:JobRun) : Option[JobState] = {
        val latestId = jobRuns
            .filter(r => r.namespace === run.namespace
                && r.project === run.project
                && r.job === run.bundle
                && r.args_hash === run.args_hash
                && r.status =!= Status.SKIPPED.value
            ).map(_.id)
            .max

        val qj = jobRuns.filter(_.id === latestId)
        val job = Await.result(db.run(qj.result), Duration.Inf)
            .headOption

        // Retrieve job arguments
        val qa = job.map(j => jobArgs.filter(_.job_id === j.id))
        val args = qa.toSeq.flatMap(q =>
            Await.result(db.run(q.result), Duration.Inf)
                .map(a => (a.name, a.value))
        ).toMap

        job.map(state => JobState(
            state.id.toString,
            state.namespace,
            state.project,
            state.bundle,
            Phase.ofString(state.phase),
            args,
            Status.ofString(state.status),
            Option(state.start_ts).map(_.toInstant.atZone(ZoneId.of("UTC"))),
            Option(state.end_ts).map(_.toInstant.atZone(ZoneId.of("UTC")))
        ))
    }

    def setJobStatus(run:JobRun) : Unit = {
        val q = jobRuns.filter(_.id === run.id).map(r => (r.end_ts, r.status)).update((run.end_ts, run.status))
        Await.result(db.run(q), Duration.Inf)
    }

    def insertJobRun(run:JobRun, args:Map[String,String]) : JobRun = {
        val runQuery = (jobRuns returning jobRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runArgs = args.map(kv => JobArgument(runResult.id, kv._1, kv._2))
        val argsQuery = jobArgs ++= runArgs
        Await.result(db.run(argsQuery), Duration.Inf)

        runResult
    }

    def findJob(query:JobQuery, order:Seq[JobOrder], limit:Int, offset:Int) : Seq[JobState] = {
        def mapOrderColumn(order:JobOrder) : JobRuns => Rep[_] = {
            order match {
                case JobOrder.BY_DATETIME => t => t.start_ts
                case JobOrder.BY_ID => t => t.id
                case JobOrder.BY_NAME => t => t.job
                case JobOrder.BY_PHASE => t => t.phase
                case JobOrder.BY_STATUS => t => t.status
            }
        }
        def mapOrderDirection(order:JobOrder) : slick.ast.Ordering = {
            if (order.isAscending)
                slick.ast.Ordering(slick.ast.Ordering.Asc)
            else
                slick.ast.Ordering(slick.ast.Ordering.Desc)
        }

        val q = query.args.foldLeft(jobRuns.map(identity))((q, kv) => q
                .join(jobArgs).on(_.id === _.job_id)
                .filter(a => a._2.name === kv._1 && a._2.value === kv._2)
                .map(xy => xy._1)
            )
            .optionalFilter(query.namespace)(_.namespace === _)
            .optionalFilter(query.project)(_.project === _)
            .optionalFilter(query.name)(_.job === _)
            .optionalFilter(query.status)(_.status === _.toString)
            .optionalFilter(query.phase)(_.phase === _.toString)
            .optionalFilter(query.from)((e,v) => e.start_ts >= Timestamp.from(v.toInstant))
            .optionalFilter(query.to)((e,v) => e.start_ts <= Timestamp.from(v.toInstant))
            .drop(offset)
            .take(limit)
            .sorted(job => new slick.lifted.Ordered(order.map(o => (mapOrderColumn(o)(job).toNode, mapOrderDirection(o))).toVector))

        Await.result(db.run(q.result), Duration.Inf)
            .map(state => JobState(
                state.id.toString,
                state.namespace,
                state.project,
                state.bundle,
                Phase.ofString(state.phase),
                Map(),
                Status.ofString(state.status),
                Option(state.start_ts).map(_.toInstant.atZone(ZoneId.of("UTC"))),
                Option(state.end_ts).map(_.toInstant.atZone(ZoneId.of("UTC")))
            ))
    }

    def getTargetState(target:TargetRun, partitions:Map[String,String]) : Option[TargetState] = {
        val latestId = targetRuns
            .filter(tr => tr.namespace === target.namespace
                && tr.project === target.project
                && tr.target === target.target
                && tr.partitions_hash === target.partitions_hash
                && tr.status =!= Status.SKIPPED.value
            )
            .map(_.id)
            .max

        // Finally select the run with the calculated ID
        val q = targetRuns.filter(r => r.id === latestId)
        val tgt = Await.result(db.run(q.result), Duration.Inf)
            .headOption

        // Retrieve target partitions
        val qp = tgt.map(t => targetPartitions.filter(_.target_id === t.id))
        val parts = qp.toSeq.flatMap(q =>
            Await.result(db.run(q.result), Duration.Inf)
                .map(a => (a.name, a.value))
        ).toMap

        tgt.map(state => TargetState(
            state.id.toString,
            state.job_id.map(_.toString),
            state.namespace,
            state.project,
            state.target,
            parts,
            Phase.ofString(state.phase),
            Status.ofString(state.status),
            Option(state.start_ts).map(_.toInstant.atZone(ZoneId.of("UTC"))),
            Option(state.end_ts).map(_.toInstant.atZone(ZoneId.of("UTC")))
        ))
    }

    def setTargetStatus(run:TargetRun) : Unit = {
        val q = targetRuns.filter(_.id === run.id).map(r => (r.end_ts, r.status)).update((run.end_ts, run.status))
        Await.result(db.run(q), Duration.Inf)
    }

    def insertTargetRun(run:TargetRun, partitions:Map[String,String]) : TargetRun = {
        val runQuery = (targetRuns returning targetRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runPartitions = partitions.map(kv => TargetPartition(runResult.id, kv._1, kv._2))
        val argsQuery = targetPartitions ++= runPartitions
        Await.result(db.run(argsQuery), Duration.Inf)

        runResult
    }
}
