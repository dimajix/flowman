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

package com.dimajix.flowman.spec.history

import java.security.MessageDigest
import java.sql.SQLRecoverableException
import java.sql.SQLTransientException
import java.sql.Timestamp
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.Locale
import java.util.Properties

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.language.higherKinds
import scala.util.control.NonFatal

import javax.xml.bind.DatatypeConverter
import org.slf4j.LoggerFactory

import com.dimajix.common.ExceptionUtils.reasons
import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.documentation
import com.dimajix.flowman.documentation.ColumnReference
import com.dimajix.flowman.documentation.ProjectDoc
import com.dimajix.flowman.documentation.Reference
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.graph.Action
import com.dimajix.flowman.history.DocumentationQuery
import com.dimajix.flowman.history.Graph
import com.dimajix.flowman.history.InputMapping
import com.dimajix.flowman.history.JobColumn
import com.dimajix.flowman.history.JobOrder
import com.dimajix.flowman.history.JobQuery
import com.dimajix.flowman.history.JobState
import com.dimajix.flowman.history.MappingNode
import com.dimajix.flowman.history.Measurement
import com.dimajix.flowman.history.MetricSeries
import com.dimajix.flowman.history.ReadRelation
import com.dimajix.flowman.history.RelationNode
import com.dimajix.flowman.history.Resource
import com.dimajix.flowman.history.TargetColumn
import com.dimajix.flowman.history.TargetOrder
import com.dimajix.flowman.history.TargetQuery
import com.dimajix.flowman.history.TargetState
import com.dimajix.flowman.history.WriteRelation
import com.dimajix.flowman.jdbc.SlickUtils
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.JobDigest
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.ResourceIdentifier
import com.dimajix.flowman.model.TargetDigest
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.types.ArrayType
import com.dimajix.flowman.types.Field
import com.dimajix.flowman.types.FieldType
import com.dimajix.flowman.types.MapType
import com.dimajix.flowman.types.StringType
import com.dimajix.flowman.types.StructType


object JdbcStateRepository {
    private val logger = LoggerFactory.getLogger(classOf[JdbcStateRepository])

    case class JobRun(
        id:Long,
        namespace: String,
        project:String,
        version:String,
        job:String,
        phase:String,
        args_hash:String,
        start_ts:Option[Timestamp],
        end_ts:Option[Timestamp],
        status:String,
        error:Option[String]
    ) {
        def name : String = {
            val ns = if (namespace.nonEmpty) namespace + "/" else ""
            val prj = if (project.nonEmpty) project + "/" else ""
            ns + prj + job
        }
    }

    case class JobArgument(
        job_id:Long,
        name:String,
        value:String
    )

    case class JobEnvironment(
        job_id:Long,
        name:String,
        value:String
    )

    case class JobMetricLabel(
        metric_id:Long,
        name:String,
        value:String
    )
    case class JobMetric(
        id:Long,
        job_id:Long,
        name:String,
        ts:Timestamp,
        value:Double
    )

    case class TargetRun(
        id:Long,
        job_id:Option[Long],
        namespace: String,
        project:String,
        version:String,
        target:String,
        phase:String,
        partitions_hash:String,
        start_ts:Option[Timestamp],
        end_ts:Option[Timestamp],
        status:String,
        error:Option[String]
    ) {
        def name : String = {
            val ns = if (namespace.nonEmpty) namespace + "/" else ""
            val prj = if (project.nonEmpty) project + "/" else ""
            ns + prj + target
        }
    }

    case class TargetPartition(
        target_id:Long,
        name:String,
        value:String
    )

    case class GraphNode(
        id:Long,
        job_id:Long,
        category:String,
        kind:String,
        name:String
    )
    case class GraphEdge(
        id:Long,
        input_id:Long,
        output_id:Long,
        action:String
    )
    case class GraphEdgeLabel(
        id:Long,
        edge_id:Long,
        name:String,
        value:String
    )
    case class GraphResource(
        id:Long,
        node_id:Long,
        direction:Char,
        category:String,
        name:String
    )
    case class GraphResourcePartition(
        resource_id:Long,
        name:String,
        value:String
    )

    case class EntityDoc(
        id:Long,
        parent_id:Option[Long],
        job_id:Long,
        category:String,
        kind:String,
        identifier:String,
        description:Option[String]
    )
    case class EntityResource(
        id:Long,
        entity_id:Long,
        category:String,
        kind:Char,
        name:String
    )
    case class EntityResourcePartition(
        resource_id:Long,
        name:String,
        value:String
    )
    case class EntityInput(
        entity_id: Long,
        input_id: Long
    )

    case class SchemaDoc(
        id:Long,
        entity_id:Long,
        description:Option[String]
    )
    case class ColumnDoc(
        id:Long,
        schema_id:Long,
        parent_id:Option[Long],
        name:String,
        data_type:String,
        nullable:Boolean,
        description:Option[String],
        index:Int
    )
    case class ColumnInput(
        column_id:Long,
        input_id:Long
    )
}


final class JdbcStateRepository(connection: JdbcStateStore.Connection, retries:Int=3, timeout:Int=1000) extends StateRepository {
    val profile = SlickUtils.getProfile(connection.driver)

    import profile.api._

    import JdbcStateRepository._

    private lazy val db = {
        val url = connection.url
        val driver = connection.driver
        val props = new Properties()
        connection.properties.foreach(kv => props.setProperty(kv._1, kv._2))
        connection.user.foreach(props.setProperty("user", _))
        connection.password.foreach(props.setProperty("password", _))
        logger.debug(s"Connecting via JDBC to $url with driver $driver")
        // Do not set username and password, since a bug in Slick would discard all other connection properties
        Database.forURL(url, driver=driver, prop=props, executor=SlickUtils.defaultExecutor)
    }

    private val jobRuns = TableQuery[JobRuns]
    private val jobArgs = TableQuery[JobArguments]
    private val jobEnvironments = TableQuery[JobEnvironments]
    private val jobMetrics = TableQuery[JobMetrics]
    private val jobMetricLabels = TableQuery[JobMetricLabels]
    private val targetRuns = TableQuery[TargetRuns]
    private val targetPartitions = TableQuery[TargetPartitions]

    private val graphEdgeLabels = TableQuery[GraphEdgeLabels]
    private val graphEdges = TableQuery[GraphEdges]
    private val graphNodes = TableQuery[GraphNodes]
    private val graphResources = TableQuery[GraphResources]
    private val graphResourcePartitions = TableQuery[GraphResourcePartitions]

    private val entityDocs = TableQuery[EntityDocs]
    private val entityResources = TableQuery[EntityResources]
    private val entityResourcePartitions = TableQuery[EntityResourcePartitions]
    private val entityInputs = TableQuery[EntityInputs]
    private val schemaDocs = TableQuery[SchemaDocs]
    private val columnDocs = TableQuery[ColumnDocs]
    private val columnInputs = TableQuery[ColumnInputs]

    private class JobRuns(tag:Tag) extends Table[JobRun](tag, "JOB_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def namespace = column[String]("namespace", O.Length(64))
        def project = column[String]("project", O.Length(64))
        def version = column[String]("version", O.Length(32))
        def job = column[String]("job", O.Length(64))
        def phase = column[String]("phase", O.Length(64))
        def args_hash = column[String]("args_hash", O.Length(32))
        def start_ts = column[Option[Timestamp]]("start_ts")
        def end_ts = column[Option[Timestamp]]("end_ts")
        def status = column[String]("status", O.Length(20))
        def error = column[Option[String]]("error", O.Length(1022))

        def idx = index("JOB_RUN_IDX", (namespace, project, job, phase, args_hash, status), unique = false)

        def * = (id, namespace, project, version, job, phase, args_hash, start_ts, end_ts, status, error) <> (JobRun.tupled, JobRun.unapply)
    }

    private class JobArguments(tag: Tag) extends Table[JobArgument](tag, "JOB_ARGUMENT") {
        def job_id = column[Long]("job_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(1022))

        def pk = primaryKey("JOB_ARGUMENT_PK", (job_id, name))
        def job = foreignKey("JOB_ARGUMENT_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (job_id, name, value) <> (JobArgument.tupled, JobArgument.unapply)
    }

    private class JobEnvironments(tag: Tag) extends Table[JobEnvironment](tag, "JOB_ENVIRONMENT") {
        def job_id = column[Long]("job_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(1022))

        def pk = primaryKey("JOB_ENVIRONMENT_PK", (job_id, name))
        def job = foreignKey("JOB_ENVIRONMENT_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (job_id, name, value) <> (JobEnvironment.tupled, JobEnvironment.unapply)
    }

    private class JobMetrics(tag: Tag) extends Table[JobMetric](tag, "JOB_METRIC") {
        def id = column[Long]("metric_id", O.PrimaryKey, O.AutoInc)
        def job_id = column[Long]("job_id")
        def name = column[String]("name", O.Length(64))
        def ts = column[Timestamp]("ts")
        def value = column[Double]("value")

        def job = foreignKey("JOB_METRIC_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, job_id, name, ts, value) <> (JobMetric.tupled, JobMetric.unapply)
    }

    private class JobMetricLabels(tag: Tag) extends Table[JobMetricLabel](tag, "JOB_METRIC_LABEL") {
        def metric_id = column[Long]("metric_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(64))

        def pk = primaryKey("JOB_METRIC_LABEL_PK", (metric_id, name))
        def metric = foreignKey("JOB_METRIC_LABEL_JOB_METRIC_FK", metric_id, jobMetrics)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
        def idx = index("JOB_METRIC_LABEL_IDX", (name, value), unique = false)

        def * = (metric_id, name, value) <> (JobMetricLabel.tupled, JobMetricLabel.unapply)
    }

    private class TargetRuns(tag: Tag) extends Table[TargetRun](tag, "TARGET_RUN") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def job_id = column[Option[Long]]("job_id")
        def namespace = column[String]("namespace", O.Length(64))
        def project = column[String]("project", O.Length(64))
        def version = column[String]("version", O.Length(32))
        def target = column[String]("target", O.Length(64))
        def phase = column[String]("phase", O.Length(12))
        def partitions_hash = column[String]("partitions_hash", O.Length(32))
        def start_ts = column[Option[Timestamp]]("start_ts")
        def end_ts = column[Option[Timestamp]]("end_ts")
        def status = column[String]("status", O.Length(20))
        def error = column[Option[String]]("error", O.Length(1022))

        def idx = index("TARGET_RUN_IDX", (namespace, project, target, phase, partitions_hash, status), unique = false)
        def job = foreignKey("TARGET_RUN_JOB_RUN_FK", job_id, jobRuns)(_.id.?, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, job_id, namespace, project, version, target, phase, partitions_hash, start_ts, end_ts, status, error) <> (TargetRun.tupled, TargetRun.unapply)
    }

    private class TargetPartitions(tag: Tag) extends Table[TargetPartition](tag, "TARGET_PARTITION") {
        def target_id = column[Long]("target_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(254))

        def pk = primaryKey("TARGET_PARTITION_PK", (target_id, name))
        def target = foreignKey("TARGET_PARTITION_TARGET_RUN_FK", target_id, targetRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (target_id, name, value) <> (TargetPartition.tupled, TargetPartition.unapply)
    }

    private class GraphEdgeLabels(tag:Tag) extends Table[GraphEdgeLabel](tag, "GRAPH_EDGE_LABEL") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def edge_id = column[Long]("edge_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(254))

        def edge = foreignKey("GRAPH_EDGE_LABEL_EDGE_FK", edge_id, graphEdges)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, edge_id, name, value) <> (GraphEdgeLabel.tupled, GraphEdgeLabel.unapply)
    }

    private class GraphEdges(tag:Tag) extends Table[GraphEdge](tag, "GRAPH_EDGE") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def input_id = column[Long]("input_id")
        def output_id = column[Long]("output_id")
        def action = column[String]("action", O.Length(16))

        def input_node = foreignKey("GRAPH_EDGE_INPUT_FK", input_id, graphNodes)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
        def output_node = foreignKey("GRAPH_EDGE_OUTPUT_FK", output_id, graphNodes)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, input_id, output_id, action) <> (GraphEdge.tupled, GraphEdge.unapply)
    }

    private class GraphNodes(tag:Tag) extends Table[GraphNode](tag, "GRAPH_NODE") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def job_id = column[Long]("job_id")
        def category = column[String]("category", O.Length(16))
        def kind = column[String]("kind", O.Length(64))
        def name = column[String]("name", O.Length(64))

        def job = foreignKey("GRAPH_NODE_JOB_FK", job_id, jobRuns)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, job_id, category, kind, name) <> (GraphNode.tupled, GraphNode.unapply)
    }

    private class GraphResources(tag:Tag) extends Table[GraphResource](tag, "GRAPH_RESOURCE") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def node_id = column[Long]("node_id")
        def direction = column[Char]("direction")
        def category = column[String]("category", O.Length(32))
        def name = column[String]("name", O.Length(254))

        def node = foreignKey("GRAPH_RESOURCE_NODE_FK", node_id, graphNodes)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, node_id, direction, category, name) <> (GraphResource.tupled, GraphResource.unapply)
    }

    private class GraphResourcePartitions(tag:Tag) extends Table[GraphResourcePartition](tag, "GRAPH_RESOURCE_PARTITION") {
        def resource_id = column[Long]("resource_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(254))

        def pk = primaryKey("GRAPH_RESOURCE_PARTITION_PK", (resource_id, name))
        def resource = foreignKey("GRAPH_RESOURCE_PARTITION_RESOURCE_FK", resource_id, graphResources)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (resource_id, name, value) <> (GraphResourcePartition.tupled, GraphResourcePartition.unapply)
    }

    private class EntityDocs(tag:Tag) extends Table[EntityDoc](tag, "ENTITY_DOC") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def parent_id = column[Option[Long]]("parent_id")
        def job_id = column[Long]("job_id")

        def category = column[String]("category", O.Length(16))
        def kind = column[String]("kind", O.Length(64))
        def identifier = column[String]("identifier", O.Length(96))
        def description = column[Option[String]]("description", O.Length(254))

        def * = (id, parent_id, job_id, category, kind, identifier, description) <> (EntityDoc.tupled, EntityDoc.unapply)
    }

    private class EntityResources(tag:Tag) extends Table[EntityResource](tag, "ENTITY_RESOURCE") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

        def entity_id = column[Long]("entity_id")
        def category = column[String]("category", O.Length(16))
        def kind = column[Char]("kind")
        def name = column[String]("name", O.Length(254))

        def entity = foreignKey("ENTITY_RESOURCE_ENTITY_FK", entity_id, entityDocs)(_.id, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)

        def * = (id, entity_id, category, kind, name) <> (EntityResource.tupled, EntityResource.unapply)
    }

    private class EntityResourcePartitions(tag: Tag) extends Table[EntityResourcePartition](tag, "ENTITY_RESOURCE_PARTITION") {
        def resource_id = column[Long]("resource_id")
        def name = column[String]("name", O.Length(64))
        def value = column[String]("value", O.Length(254))

        def pk = primaryKey("ENTITY_RESOURCE_PARTITION_PK", (resource_id, name))
        def resource = foreignKey("ENTITY_RESOURCE_PARTITION_RESOURCE_FK", resource_id, entityResources)(_.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)

        def * = (resource_id, name, value) <> (EntityResourcePartition.tupled, EntityResourcePartition.unapply)
    }

    private class EntityInputs(tag: Tag) extends Table[EntityInput](tag, "ENTITY_INPUT") {
        def entity_id = column[Long]("entity_id")
        def input_id = column[Long]("input_id")

        def pk = primaryKey("ENTITY_INPUT_PK", (entity_id, input_id))

        def * = (entity_id, input_id) <> (EntityInput.tupled, EntityInput.unapply)
    }

    private class SchemaDocs(tag: Tag) extends Table[SchemaDoc](tag, "SCHEMA_DOC") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def entity_id = column[Long]("entity_id")
        def description = column[Option[String]]("description", O.Length(254))

        def entity = foreignKey("SCHEMA_DOC_SCHEMA_DOC_FK", entity_id, entityDocs)(_.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)
        def * = (id, entity_id, description) <> (SchemaDoc.tupled, SchemaDoc.unapply)
    }

    private class ColumnDocs(tag: Tag) extends Table[ColumnDoc](tag, "COLUMN_DOC") {
        def id = column[Long]("id", O.PrimaryKey, O.AutoInc)
        def schema_id = column[Long]("schema_id")
        def parent_id = column[Option[Long]]("parent_id")

        def name = column[String]("name", O.Length(64))
        def data_type = column[String]("data_type", O.Length(64))
        def nullable = column[Boolean]("nullable")
        def index = column[Int]("index")
        def description = column[Option[String]]("description", O.Length(254))

        def entity = foreignKey("COLUMN_DOC_SCHEMA_DOC_FK", schema_id, entityDocs)(_.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)
        def parent = foreignKey("COLUMN_DOC_PARENT_FK", parent_id, columnDocs)(_.id.?, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)

        def * = (id, schema_id, parent_id, name, data_type, nullable, description, index) <> (ColumnDoc.tupled, ColumnDoc.unapply)
    }

    private class ColumnInputs(tag: Tag) extends Table[ColumnInput](tag, "COLUMN_INPUT") {
        def column_id = column[Long]("column_id")
        def input_id = column[Long]("input_id")

        def pk = primaryKey("COLUMN_INPUT_PK", (column_id, input_id))

        def * = (column_id, input_id) <> (ColumnInput.tupled, ColumnInput.unapply)
    }


    implicit private class QueryWrapper[E,U,C[_]] (query:Query[E, U, C]) {
        def optionalFilter[T](value:Option[T])(f:(E,T) => Rep[Boolean]) : Query[E,U,C] = {
            if (value.nonEmpty)
                query.filter(v => f(v,value.get))
            else
                query
        }
        def optionalFilter[T](value:Seq[T])(f:(E,Seq[T]) => Rep[Boolean]) : Query[E,U,C] = {
            if (value.nonEmpty)
                query.filter(v => f(v,value))
            else
                query
        }
    }

    override def create() : Unit = {
        import scala.concurrent.ExecutionContext.Implicits.global
        val tables = Seq(
            jobRuns,
            jobArgs,
            jobEnvironments,
            jobMetrics,
            jobMetricLabels,
            targetRuns,
            targetPartitions,
            graphNodes,
            graphEdges,
            graphEdgeLabels,
            graphResources,
            graphResourcePartitions,
            entityDocs,
            entityResources,
            entityResourcePartitions,
            schemaDocs,
            columnDocs,
            columnInputs
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
            case NonFatal(ex) => logger.error(s"Cannot create tables of JDBC history database at '${connection.url}':\n  ${reasons(ex)}")
        }
    }

    override def withRetry[T](fn: => T): T = {
        def retry[T](n: Int)(fn: => T): T = {
            try {
                fn
            } catch {
                case e@(_: SQLRecoverableException | _: SQLTransientException) if n > 1 =>
                    logger.warn("Retrying after error while executing SQL: {}", e.getMessage)
                    Thread.sleep(timeout)
                    retry(n - 1)(fn)
            }
        }

        retry(retries) {
            fn
        }
    }

    override def getJobState(job:JobDigest) : Option[JobState] = {
        val latestId = jobRuns
            .filter(r => r.namespace === job.namespace
                && r.project === job.project
                && r.job === job.job
                && r.args_hash === hashMap(job.args)
                && r.status =!= Status.SKIPPED.toString
            ).map(_.id)
            .max

        val qj = jobRuns.filter(_.id === latestId)
        val job0 = Await.result(db.run(qj.result), Duration.Inf)
            .headOption

        // Retrieve job arguments
        val qa = job0.map(j => jobArgs.filter(_.job_id === j.id))
        val args = qa.toSeq.flatMap(q =>
            Await.result(db.run(q.result), Duration.Inf)
                .map(a => (a.name, a.value))
        ).toMap

        job0.map(state => JobState(
            state.id.toString,
            state.namespace,
            state.project,
            state.version,
            state.job,
            Phase.ofString(state.phase),
            args,
            Status.ofString(state.status),
            state.start_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
            state.end_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
            state.error
        ))
    }

    override def updateJobState(state:JobState) : Unit = {
        val q = jobRuns.filter(_.id === state.id.toLong)
            .map(r => (r.end_ts, r.status, r.error))
            .update((state.endDateTime.map(ts => new Timestamp(ts.toInstant.toEpochMilli)), state.status.upper, state.error.map(_.take(1021))))
        Await.result(db.run(q), Duration.Inf)
    }

    override def insertJobState(state:JobState, env:Map[String,String]) : JobState = {
        val run = JobRun(
            0,
            state.namespace,
            state.project,
            state.version,
            state.job,
            state.phase.upper,
            hashMap(state.args),
            state.startDateTime.map(st => new Timestamp(st.toInstant.toEpochMilli)),
            state.endDateTime.map(st => new Timestamp(st.toInstant.toEpochMilli)),
            state.status.upper,
            None
        )

        val runQuery = (jobRuns returning jobRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runArgs = state.args.map(kv => JobArgument(runResult.id, kv._1, kv._2))
        val argsQuery = jobArgs ++= runArgs
        Await.result(db.run(argsQuery), Duration.Inf)

        val runEnvs = env.map(kv => JobEnvironment(runResult.id, kv._1, kv._2))
        val envsQuery = jobEnvironments ++= runEnvs
        Await.result(db.run(envsQuery), Duration.Inf)

        state.copy(id = runResult.id.toString)
    }

    override def insertJobMetrics(jobId:String, metrics:Seq[Measurement]) : Unit = {
        implicit val ec = db.executor.executionContext

        val result = metrics.map { m =>
            val jobMetric = JobMetric(0, jobId.toLong, m.name, new Timestamp(m.ts.toInstant.toEpochMilli), m.value)
            val jmQuery = (jobMetrics returning jobMetrics.map(_.id) into((jm,id) => jm.copy(id=id))) += jobMetric
            db.run(jmQuery).flatMap { metric =>
                val labels = m.labels.map(l => JobMetricLabel(metric.id, l._1, l._2))
                val mlQuery = jobMetricLabels ++= labels
                db.run(mlQuery)
            }
        }

        Await.result(Future.sequence(result), Duration.Inf)
    }

    override def getJobMetrics(jobId:String) : Seq[Measurement] = {
        // Ensure that job actually exists
        ensureJobExists(jobId)

        // Now query metrics
        val q = jobMetrics.filter(_.job_id === jobId.toLong)
            .joinLeft(jobMetricLabels).on(_.id === _.metric_id)
        Await.result(db.run(q.result), Duration.Inf)
            .groupBy(_._1.id)
            .map { g =>
                val metric = g._2.head._1
                val labels = g._2.flatMap(_._2).map(kv => kv.name -> kv.value).toMap
                Measurement(metric.name, metric.job_id.toString, metric.ts.toInstant.atZone(ZoneId.systemDefault()), labels, metric.value)
            }
            .toSeq
    }

    override def getJobEnvironment(jobId:String) : Map[String,String] = {
        // Ensure that job actually exists
        ensureJobExists(jobId)

        // Now query metrics
        val q = jobEnvironments.filter(_.job_id === jobId.toLong)
        Await.result(db.run(q.result), Duration.Inf)
            .map(e => e.name -> e.value)
            .toMap
    }

    override def insertJobGraph(jobId:String, graph:Graph) : Unit = {
        // Step 1: Insert nodes
        val dbNodes = graph.nodes.map(n => GraphNode(-1, jobId.toLong, n.category.lower, n.kind, n.identifier.toString))
        val nodesQuery = (graphNodes returning graphNodes.map(_.id)) ++= dbNodes
        val nodeIds = Await.result(db.run(nodesQuery), Duration.Inf)
        val nodeIdToDbId = graph.nodes.map(_.id).zip(nodeIds).toMap

        // Step 2: Insert edges
        val dbEdges = graph.edges.map(e => GraphEdge(-1, nodeIdToDbId(e.input.id), nodeIdToDbId(e.output.id), e.action.upper))
        val edgesQuery = (graphEdges returning graphEdges.map(_.id)) ++= dbEdges
        val edgeIds = Await.result(db.run(edgesQuery), Duration.Inf)

        // Step 3: Insert edge labels
        val dbLabels = graph.edges.zip(edgeIds).flatMap { case (e,id) => e.labels.flatMap(l => l._2.map(v => GraphEdgeLabel(-1, id, l._1, v))) }
        val labelsQuery = graphEdgeLabels ++= dbLabels
        Await.result(db.run(labelsQuery), Duration.Inf)

        // Step 4: Insert node resources
        val dbResources = graph.nodes.flatMap { n =>
               n.provides.map(r => (GraphResource(-1, nodeIdToDbId(n.id), 'O', r.category, r.name), r.partition)) ++
                   n.requires.map(r => (GraphResource(-1, nodeIdToDbId(n.id), 'I', r.category, r.name), r.partition))
            }
        val resourceQuery = (graphResources returning graphResources.map(_.id)) ++= dbResources.map(_._1)
        val resourceIds = Await.result(db.run(resourceQuery), Duration.Inf)

        // Step 5: Insert node resource partitions
        val dbResourcePartitions = dbResources.map(_._2).zip(resourceIds).flatMap(r => r._1.map(kv => GraphResourcePartition(r._2, kv._1, kv._2)))
        val resourcePartitionsQuery = graphResourcePartitions ++= dbResourcePartitions
        Await.result(db.run(resourcePartitionsQuery), Duration.Inf)
    }

    override def getJobGraph(jobId:String) : Option[Graph] = {
        // Ensure that job actually exists
        ensureJobExists(jobId)

        // Now retrieve graph nodes
        val qn = graphNodes.filter(_.job_id === jobId.toLong)
        val dbNodes = Await.result(db.run(qn.result), Duration.Inf)

        // Only build graph if nodes are present, otherwise return None
        if (dbNodes.nonEmpty) {
            val qr = graphResources.filter(r => r.node_id.in(qn.map(_.id).distinct))
                .joinLeft(graphResourcePartitions).on(_.id === _.resource_id)
            val dbResources = Await.result(db.run(qr.result), Duration.Inf)
            val resourcesById = dbResources.groupBy(_._1.node_id).map { case(nodeId,res) =>
                val resources = res.groupBy(_._1.id).toSeq.map { case (resourceId,parts) =>
                    val resource = parts.head._1
                    val partitions = parts.flatMap(p => p._2).map(kv => kv.name -> kv.value).toMap
                    resource.direction -> Resource(resource.category, resource.name, partitions)
                }
                val provides = resources.filter(_._1 == 'O').map(_._2)
                val requires = resources.filter(_._1 == 'I').map(_._2)
                nodeId -> ((provides, requires))
            }

            val graph = Graph.builder()
            val nodesById = dbNodes.map { r =>
                val resources = resourcesById.get(r.id)
                val provides = resources.toSeq.flatMap(_._1)
                val requires = resources.toSeq.flatMap(_._2)
                val node = Category.ofString(r.category) match {
                    case Category.MAPPING => graph.newMappingNode(MappingIdentifier(r.name), r.kind, requires)
                    case Category.TARGET => graph.newTargetNode(TargetIdentifier(r.name), r.kind, provides, requires)
                    case Category.RELATION => graph.newRelationNode(RelationIdentifier(r.name), r.kind, provides, requires)
                    case _ => throw new IllegalArgumentException(s"Unsupported tye ${r.category}")
                }
                r.id -> node
            }.toMap

            val qe = graphEdges.filter(e => e.input_id.in(qn.map(_.id).distinct))
                .joinLeft(graphEdgeLabels).on(_.id === _.edge_id)
            val dbEdges = Await.result(db.run(qe.result), Duration.Inf)
            dbEdges.groupBy(_._1.id).foreach { case (x,y) =>
                val edge = y.head._1
                val labels = y.flatMap(_._2)
                    .map(l => l.name -> l.value)
                    .groupBy(_._1)
                    .map(l => l._1 -> l._2.map(_._2))

                val inputNode = nodesById(edge.input_id)
                val outputNode = nodesById(edge.output_id)
                val edge2 = Action.ofString(edge.action) match {
                    case Action.INPUT =>
                        InputMapping(inputNode.asInstanceOf[MappingNode], outputNode, labels("pin").head)
                    case Action.READ =>
                        ReadRelation(inputNode.asInstanceOf[RelationNode], outputNode, labels)
                    case Action.WRITE =>
                        WriteRelation(inputNode, outputNode.asInstanceOf[RelationNode], labels.map(kv => kv._1 -> kv._2.head))
                }
                graph.addEdge(edge2)
            }

            Some(graph.build())
        }
        else {
            None
        }
    }

    override def insertJobDocumentation(jobId: String, doc: ProjectDoc): Unit = {
        def createEntityResources(entityId: Long, kind: Char, resources: Seq[ResourceIdentifier]): Seq[EntityResource] = {
            resources.map(res => EntityResource(-1, entityId, res.category, kind, res.name))
        }
        def createObjectIndex[T](ids:Iterable[Long], entites:Iterable[T]) : IdentityHashMap[T,Long] = {
            val entityToId = new IdentityHashMap[T, Long]()
            entites.zip(ids).foreach(nid => entityToId.put(nid._1, nid._2))
            entityToId
        }

        // Insert all relations
        val dbRelations = doc.relations.map(n => EntityDoc(-1, None, jobId.toLong, n.category.lower, n.kind, n.identifier.toString, n.description))
        val relationQuery = (entityDocs returning entityDocs.map(_.id)) ++= dbRelations
        val relationIds = Await.result(db.run(relationQuery), Duration.Inf)
        val relationToId = createObjectIndex(relationIds, doc.relations)

        // Insert all resources
        val dbResources = doc.relations.flatMap { rel =>
            val relId = relationToId.getOrElse(rel, throw new IllegalStateException())
            createEntityResources(relId, 'P', rel.provides) ++
                createEntityResources(relId, 'R', rel.requires) ++
                createEntityResources(relId, 'S', rel.sources)
        }
        val resourceQuery = (entityResources returning entityResources.map(_.id)) ++= dbResources
        val resourceIds = Await.result(db.run(resourceQuery), Duration.Inf)
        val resourcePartitions = doc.relations.flatMap { rel =>
            (rel.provides ++ rel.requires ++ rel.sources)
        }
        val resourcePartitionQuery = entityResourcePartitions ++= resourcePartitions.zip(resourceIds).flatMap {
            case (res, id) => res.partition.toSeq.map(kv => EntityResourcePartition(id, kv._1, kv._2))
        }
        Await.result(db.run(resourcePartitionQuery), Duration.Inf)

        // Insert all schemas
        val dbSchemas = doc.relations.flatMap { rel =>
            val relId = relationToId.getOrElse(rel, throw new IllegalStateException())
            rel.schema.map(s => SchemaDoc(-1, relId, s.description))
        }
        val schemaQuery = (schemaDocs returning schemaDocs.map(_.id)) ++= dbSchemas
        val schemaIds = Await.result(db.run(schemaQuery), Duration.Inf)

        val columnToId = new IdentityHashMap[com.dimajix.flowman.documentation.ColumnDoc, Long]()

        @tailrec
        def insertColumns(columns:Seq[(Long,Option[Long],com.dimajix.flowman.documentation.ColumnDoc)]) : Unit = {
            val dbColumns = columns.map { case (schemaId, parentId, col) =>
                ColumnDoc(-1, schemaId, parentId, col.name, col.typeName, col.nullable, col.description, col.index)
            }
            val columnQuery = (columnDocs returning columnDocs.map(_.id)) ++= dbColumns
            val columnIds = Await.result(db.run(columnQuery), Duration.Inf)

            val columnsWithId = columns.zip(columnIds)
            columnsWithId.foreach { case ((_, _, col), columnId) =>
                columnToId.put(col, columnId)
            }
            val newColumns = columnsWithId
                .filter(_._1._3.children.nonEmpty)
                .map { case((schemaId, parentId, col), id) => (schemaId, Some(id), col) }

            if (newColumns.nonEmpty)
                insertColumns(newColumns)
        }

        // Insert all columns
        val allColumns = doc.relations.flatMap(_.schema).zip(schemaIds)
            .flatMap { case(schema,schemaId) =>
                schema.columns.map(col => (schemaId, None, col))
            }
        insertColumns(allColumns)

        // Insert all column references
        @tailrec
        def createColumnInputs(columns:Seq[com.dimajix.flowman.documentation.ColumnDoc], other:Seq[ColumnInput]=Seq.empty) : Seq[ColumnInput] = {
            val inputs = columns.flatMap { col =>
                val colId = columnToId.getOrElse(col, throw new IllegalStateException())
                col.inputs.flatMap { ref =>
                    doc.resolve(ref) match {
                        case Some(cr: com.dimajix.flowman.documentation.ColumnDoc) =>
                            columnToId.get(cr).map(refId => ColumnInput(colId, refId))
                        case _ => None
                    }
                }
            }
            val children = columns.flatMap(_.children)
            if (children.isEmpty)
                other ++ inputs
            else
                createColumnInputs(children, other ++ inputs)
        }
        val dbColumnInputs = doc.relations.flatMap(_.schema).flatMap(schema => createColumnInputs(schema.columns))
        val columnInputQuery = columnInputs ++= dbColumnInputs
        Await.result(db.run(columnInputQuery), Duration.Inf)

        // Insert all partitions
        //doc.relations.flatMap { rel =>
        //    val dbId = relationToId.get(rel)
        //}
    }

    private def getRelationDocumentation(projectRef:Reference, dbEntities:Seq[EntityDoc], resourcesById:Map[Long, Seq[(Char,ResourceIdentifier)]]) = {
        // Create all relations
        dbEntities.filter(_.category == com.dimajix.flowman.documentation.Category.RELATION.lower)
            .map { rel =>
                val resources = resourcesById.get(rel.id)
                rel.id -> com.dimajix.flowman.documentation.RelationDoc(
                    relation = None,
                    parent = Some(projectRef),
                    kind = rel.kind,
                    identifier = RelationIdentifier(rel.identifier),
                    description = rel.description,
                    schema = None,
                    inputs = Seq.empty,
                    provides = resources.toSeq.flatMap(_.filter(_._1 == 'P').map(_._2)),
                    requires = resources.toSeq.flatMap(_.filter(_._1 == 'R').map(_._2)),
                    sources = resources.toSeq.flatMap(_.filter(_._1 == 'S').map(_._2)),
                    partitions = Map.empty
                )
            }.toMap
    }
    private def getSchemaDocs(dbSchemas:Seq[SchemaDoc], dbColumns:Seq[ColumnDoc], dbColumnInputs:Map[Long,Seq[ColumnInput]], entitiesById:Map[Long,com.dimajix.flowman.documentation.EntityDoc]) = {
        val schemasById = dbSchemas
            .map { schema =>
                val doc = com.dimajix.flowman.documentation.SchemaDoc(
                    parent = entitiesById.get(schema.entity_id).map(_.reference),
                    description = schema.description
                )
                schema.id -> ((schema.entity_id, doc))
            }.toMap

        val dbColumnsBySchema = dbColumns.filter(_.parent_id.isEmpty).groupBy(col => col.schema_id)
        val dbColumnsByParent = dbColumns.groupBy(col => col.parent_id.getOrElse(-1))

        val columnsByIdentity = new IdentityHashMap[com.dimajix.flowman.documentation.ColumnDoc, Long]()
        val columnRefsById = mutable.Map[Long, ColumnReference]()

        def buildColumn(col: ColumnDoc, parent: Option[Reference]): com.dimajix.flowman.documentation.ColumnDoc = {
            val ftype = col.data_type.toLowerCase(Locale.ROOT) match {
                case "map" => MapType(StringType, StringType)
                case "struct" => StructType(Seq.empty)
                case "array" => ArrayType(StringType)
                case _ => FieldType.of(col.data_type)
            }
            val field0 = Field(col.name, ftype, col.nullable, description = col.description)
            val column = com.dimajix.flowman.documentation.ColumnDoc(
                parent,
                field0,
                Seq.empty,
                Seq.empty,
                Seq.empty,
                col.index
            )
            val ref = column.reference

            val children = dbColumnsByParent.get(col.id)
                .toSeq
                .flatMap(_.map(c => buildColumn(c, Some(ref))))
                .sortBy(_.index)
            val field = ftype match {
                case _: MapType if children.nonEmpty => field0.copy(ftype = MapType(StringType, children.head.field.ftype))
                case _: StructType => field0.copy(ftype = StructType(children.map(_.field)))
                case _: ArrayType if children.nonEmpty => field0.copy(ftype = ArrayType(children.head.field.ftype))
                case _ => field0
            }

            val result = column.copy(field = field, children = children)
            columnsByIdentity.put(result, col.id)
            columnRefsById.put(col.id, ref)
            result
        }

        val columnsBySchema = dbColumnsBySchema
            .flatMap { case (schemaId, cols) =>
                schemasById.get(schemaId).map { case(_,schema) =>
                    val parent = Some(schema.reference)
                    val docs = cols
                        .map { col =>
                            buildColumn(col, parent)
                        }
                        .sortBy(_.index)
                    schemaId -> docs
                }
            }

        def connectColumn(col: com.dimajix.flowman.documentation.ColumnDoc): com.dimajix.flowman.documentation.ColumnDoc = {
            val children = col.children.map(connectColumn)
            val id = columnsByIdentity.getOrElse(col, throw new IllegalStateException())
            val inputs = dbColumnInputs.get(id)
                .toSeq
                .flatMap(_.flatMap(inputId => columnRefsById.get(inputId.input_id)))
            col.copy(children = children, inputs = inputs)
        }

        columnsBySchema.map { case (schemaId, cols) =>
            val (entityId,schema) = schemasById.getOrElse(schemaId, throw new IllegalStateException())
            entityId -> schema.copy(columns = cols.map(connectColumn))
        }
    }

    def getJobDocumentation(jobId:String) : Option[ProjectDoc] = {
        // Ensure that job actually exists
        val jobRun = ensureJobExists(jobId)
        val project = com.dimajix.flowman.documentation.ProjectDoc(
            jobRun.project,
            Some(jobRun.version).filter(_.nonEmpty)
        )
        val projectRef = project.reference

        // Now retrieve entities
        val qn = entityDocs.filter(_.job_id === jobId.toLong)
        val dbEntities = Await.result(db.run(qn.result), Duration.Inf)

        val qr = entityResources.filter(r => r.entity_id.in(qn.map(_.id).distinct))
            .joinLeft(entityResourcePartitions).on(_.id === _.resource_id)
        val dbResources = Await.result(db.run(qr.result), Duration.Inf)
        val resourcesByEntityId = dbResources.groupBy(_._1.entity_id).map { case (entityId, res) =>
            val resources = res.groupBy(_._1.id).toSeq.map { case (resourceId, parts) =>
                val resource = parts.head._1
                val partitions = parts.flatMap(p => p._2).map(kv => kv.name -> kv.value).toMap
                resource.kind -> ResourceIdentifier(resource.category, resource.name, partitions)
            }
            entityId -> resources
        }

        // Create all relations
        val relations0 = getRelationDocumentation(projectRef, dbEntities, resourcesByEntityId)

        // Retrieve schemas
        val qs = schemaDocs.filter(r => r.entity_id.in(qn.map(_.id).distinct))
        val dbSchemas = Await.result(db.run(qs.result), Duration.Inf)
        val qc = columnDocs.filter(r => r.schema_id.in(qs.map(_.id).distinct))
        val dbColumns = Await.result(db.run(qc.result), Duration.Inf)
        val qci = columnInputs.filter(r => r.column_id.in(qc.map(_.id).distinct))
        val dbColumnInputs = Await.result(db.run(qci.result), Duration.Inf)
            .groupBy(_.column_id)

        val schemas = getSchemaDocs(dbSchemas, dbColumns, dbColumnInputs, relations0)

        val relations = relations0.map { case(id,rel) =>
            schemas.get(id).map(schema => rel.copy(schema = Some(schema))).getOrElse(rel)
        }

        Some(project.copy(
            relations = relations.toSeq
        ))
    }

    private def queryJobs(query:JobQuery) : Query[JobRuns,JobRun,Seq]  = {
        query.args.foldLeft(jobRuns.map(identity))((q, kv) => q
                .join(jobArgs).on(_.id === _.job_id)
                .filter(a => a._2.name === kv._1 && a._2.value === kv._2)
                .map(xy => xy._1)
            )
            .optionalFilter(query.id)((l,r) => l.id.inSet(r.map(_.toLong)))
            .optionalFilter(query.namespace)((l,r) => l.namespace.inSet(r))
            .optionalFilter(query.project)((l,r) => l.project.inSet(r))
            .optionalFilter(query.job)((l,r) => l.job.inSet(r))
            .optionalFilter(query.status)((l,r) => l.status.inSet(r.map(_.toString)))
            .optionalFilter(query.phase)((l,r) => l.phase.inSet(r.map(_.toString)))
            .optionalFilter(query.from)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) >= Timestamp.from(v.toInstant))
            .optionalFilter(query.to)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) <= Timestamp.from(v.toInstant))
    }

    override def findJobs(query:JobQuery, order:Seq[JobOrder], limit:Int, offset:Int) : Seq[JobState] = {
        def mapOrderColumn(order:JobOrder) : JobRuns => Rep[_] = {
            order.column match {
                case JobColumn.DATETIME => t => t.start_ts
                case JobColumn.ID => t => t.id
                case JobColumn.PROJECT => t => t.project
                case JobColumn.NAME => t => t.job
                case JobColumn.PHASE => t => t.phase
                case JobColumn.STATUS => t => t.status
            }
        }
        def mapOrderDirection(order:JobOrder) : slick.ast.Ordering = {
            if (order.isAscending)
                slick.ast.Ordering(slick.ast.Ordering.Asc)
            else
                slick.ast.Ordering(slick.ast.Ordering.Desc)
        }
        val ordering =
            (l:JobState, r:JobState) => {
                order.map { o =>
                    val result = o.column match {
                        case JobColumn.DATETIME => cmpOpt(l.startDateTime, r.startDateTime)(cmpDt)
                        case JobColumn.ID => cmpStr(l.id, r.id)
                        case JobColumn.PROJECT => cmpStr(l.project, r.project)
                        case JobColumn.NAME => cmpStr(l.job, r.job)
                        case JobColumn.PHASE => cmpStr(l.phase.toString, r.phase.toString)
                        case JobColumn.STATUS => cmpStr(l.status.toString, r.status.toString)
                    }
                    if (o.isAscending)
                        result
                    else
                        -result
                }
                .find(_ != 0)
                .exists(_ < 0)
            }

        val q = queryJobs(query)
            .sorted(job => new slick.lifted.Ordered(order.map(o => (mapOrderColumn(o)(job).toNode, mapOrderDirection(o))).toVector))
            .drop(offset)
            .take(limit)
            .joinLeft(jobArgs).on(_.id === _.job_id)

        Await.result(db.run(q.result), Duration.Inf)
            .groupBy(_._1.id)
            .values
            .map { states =>
                val state = states.head._1
                val args = states.flatMap(_._2)
                JobState(
                    state.id.toString,
                    state.namespace,
                    state.project,
                    state.version,
                    state.job,
                    Phase.ofString(state.phase),
                    args.map(a => a.name -> a.value).toMap,
                    Status.ofString(state.status),
                    state.start_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
                    state.end_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
                    state.error
                )
            }
            .toSeq
            .sortWith(ordering)
    }

    override def countJobs(query:JobQuery) : Int = {
        val q = queryJobs(query)
            .length

        Await.result(db.run(q.result), Duration.Inf)
    }

    override def countJobs(query:JobQuery, grouping:JobColumn) : Seq[(String,Int)] = {
        def mapGroupingColumn(column:JobColumn) : JobRuns => Rep[String] = {
            column match {
                case JobColumn.DATETIME => t => t.start_ts.asColumnOf[String]
                case JobColumn.ID => t => t.id.asColumnOf[String]
                case JobColumn.PROJECT => t => t.project
                case JobColumn.NAME => t => t.job
                case JobColumn.PHASE => t => t.phase
                case JobColumn.STATUS => t => t.status
            }
        }

        val q = queryJobs(query)
            .groupBy(j => mapGroupingColumn(grouping)(j))
            .map(kv => kv._1 -> kv._2.length)

        Await.result(db.run(q.result), Duration.Inf)
    }

    override def getTargetState(target:TargetDigest) : Option[TargetState] = {
        val q = targetRuns
            .filter(tr => tr.namespace === target.namespace
                && tr.project === target.project
                && tr.target === target.target
                && tr.partitions_hash === hashMap(target.partitions)
                && tr.status =!= Status.SKIPPED.toString
            )
            .map(_.id)
            .max

        Await.result(db.run(q.result), Duration.Inf)
            .map { id => getTargetState(id.toString) }
    }

    override def getTargetState(targetId:String) : TargetState = {
        // Finally select the run with the calculated ID
        val state = ensureTargetExists(targetId)

        // Retrieve target partitions
        val qp = targetPartitions.filter(_.target_id === state.id)
        val parts = Await.result(db.run(qp.result), Duration.Inf)
            .map(a => a.name -> a.value)
            .toMap

        TargetState(
            state.id.toString,
            state.job_id.map(_.toString),
            state.namespace,
            state.project,
            state.version,
            state.target,
            parts,
            Phase.ofString(state.phase),
            Status.ofString(state.status),
            state.start_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
            state.end_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
            state.error
        )
    }

    override def updateTargetState(run:TargetState) : Unit = {
        val q = targetRuns.filter(_.id === run.id.toLong)
            .map(r => (r.end_ts, r.status, r.error))
            .update((run.endDateTime.map(ts => new Timestamp(ts.toInstant.toEpochMilli)), run.status.upper, run.error.map(_.take(1021))))
        Await.result(db.run(q), Duration.Inf)
    }

    override def insertTargetState(state:TargetState) : TargetState = {
        val run = TargetRun(
            0,
            state.jobId.map(_.toLong),
            state.namespace,
            state.project,
            state.project,
            state.target,
            state.phase.upper,
            hashMap(state.partitions),
            state.startDateTime.map(ts => new Timestamp(ts.toInstant.toEpochMilli)),
            state.endDateTime.map(ts => new Timestamp(ts.toInstant.toEpochMilli)),
            state.status.upper,
            state.error
        )

        val runQuery = (targetRuns returning targetRuns.map(_.id) into((run, id) => run.copy(id=id))) += run
        val runResult = Await.result(db.run(runQuery), Duration.Inf)

        val runPartitions = state.partitions.map(kv => TargetPartition(runResult.id, kv._1, kv._2))
        val argsQuery = targetPartitions ++= runPartitions
        Await.result(db.run(argsQuery), Duration.Inf)

        state.copy(id=runResult.id.toString)
    }

    private def queryTargets(query:TargetQuery) : Query[TargetRuns, TargetRun, Seq] = {
        query.partitions.foldLeft(targetRuns.map(identity))((q, kv) => q
            .join(targetPartitions).on(_.id === _.target_id)
            .filter(a => a._2.name === kv._1 && a._2.value === kv._2)
            .map(xy => xy._1)
        )
            .optionalFilter(query.id)((l,r) => l.id.inSet(r.map(_.toLong)))
            .optionalFilter(query.namespace)((l,r) => l.namespace.inSet(r))
            .optionalFilter(query.project)((l,r) => l.project.inSet(r))
            .optionalFilter(query.target)((l,r) => l.target.inSet(r))
            .optionalFilter(query.status)((l,r) => l.status.inSet(r.map(_.toString)))
            .optionalFilter(query.phase)((l,r) => l.phase.inSet(r.map(_.toString)))
            .optionalFilter(query.jobId)((e,v) => e.job_id.inSet(v.map(_.toLong)).getOrElse(false))
            .optionalFilter(query.from)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) >= Timestamp.from(v.toInstant))
            .optionalFilter(query.to)((e,v) => e.start_ts.getOrElse(new Timestamp(0)) <= Timestamp.from(v.toInstant))
    }

    private def mapTargetColumn(column:TargetColumn) : TargetRuns => Rep[_] = {
        column match {
            case TargetColumn.DATETIME => t => t.start_ts
            case TargetColumn.ID => t => t.id
            case TargetColumn.PROJECT => t => t.project
            case TargetColumn.NAME => t => t.target
            case TargetColumn.PHASE => t => t.phase
            case TargetColumn.STATUS => t => t.status
            case TargetColumn.PARENT_ID => t => t.job_id
        }
    }


    override def findTargets(query:TargetQuery, order:Seq[TargetOrder], limit:Int, offset:Int) : Seq[TargetState] = {
        def mapOrderDirection(order:TargetOrder) : slick.ast.Ordering = {
            if (order.isAscending)
                slick.ast.Ordering(slick.ast.Ordering.Asc)
            else
                slick.ast.Ordering(slick.ast.Ordering.Desc)
        }
        val ordering =
            (l:TargetState, r:TargetState) => {
                order.map { o =>
                    val result = o.column match {
                        case TargetColumn.DATETIME => cmpOpt(l.startDateTime, r.startDateTime)(cmpDt)
                        case TargetColumn.ID => cmpStr(l.id, r.id)
                        case TargetColumn.PROJECT => cmpStr(l.project, r.project)
                        case TargetColumn.NAME => cmpStr(l.target, r.target)
                        case TargetColumn.PHASE => cmpStr(l.phase.toString, r.phase.toString)
                        case TargetColumn.STATUS => cmpStr(l.status.toString, r.status.toString)
                        case TargetColumn.PARENT_ID => cmpOpt(l.jobId, r.jobId)(cmpStr)
                    }
                    if (o.isAscending)
                        result
                    else
                        -result
                }
                .find(_ != 0)
                .exists(_ < 0)
            }

        val q = queryTargets(query)
            .sorted(job => new slick.lifted.Ordered(order.map(o => (mapTargetColumn(o.column)(job).toNode, mapOrderDirection(o))).toVector))
            .drop(offset)
            .take(limit)
            .joinLeft(targetPartitions).on(_.id === _.target_id)

        Await.result(db.run(q.result), Duration.Inf)
            .groupBy(_._1.id)
            .values
            .map { states =>
                val state = states.head._1
                val partitions = states.flatMap(_._2)
                TargetState(
                    state.id.toString,
                    state.job_id.map(_.toString),
                    state.namespace,
                    state.project,
                    state.version,
                    state.target,
                    partitions.map(t => t.name -> t.value).toMap,
                    Phase.ofString(state.phase),
                    Status.ofString(state.status),
                    state.start_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
                    state.end_ts.map(_.toInstant.atZone(ZoneId.systemDefault())),
                    state.error
                )
            }
            .toSeq
            .sortWith(ordering)
    }

    override def countTargets(query:TargetQuery) : Int = {
        val q = queryTargets(query)
            .length

        Await.result(db.run(q.result), Duration.Inf)
    }

    override def countTargets(query:TargetQuery, grouping:TargetColumn) : Seq[(String,Int)] = {
        def mapGroupingColumn(column:TargetColumn) : TargetRuns => Rep[String] = {
            column match {
                case TargetColumn.DATETIME => t => t.start_ts.asColumnOf[String]
                case TargetColumn.ID => t => t.id.asColumnOf[String]
                case TargetColumn.PROJECT => t => t.project
                case TargetColumn.NAME => t => t.target
                case TargetColumn.PHASE => t => t.phase
                case TargetColumn.STATUS => t => t.status
                case TargetColumn.PARENT_ID => t => t.job_id.asColumnOf[String]
            }
        }

        val q = queryTargets(query)
            .groupBy(j => mapGroupingColumn(grouping)(j))
            .map(kv => kv._1 -> kv._2.length)

        Await.result(db.run(q.result), Duration.Inf)
    }

    override def findMetrics(jobQuery: JobQuery, groupings:Seq[String]) : Seq[MetricSeries] = {
        // Select metrics according to jobQuery
        val q0 = queryJobs(jobQuery)
            .join(jobMetrics).on(_.id === _.job_id)
            .map(_._2)

        // Select metrics according to required grouping labels
        val q1 = groupings.foldLeft(q0)((jm,g) =>
            jm.join(jobMetricLabels).on(_.id === _.metric_id)
                .filter(_._2.name === g)
                .map(_._1)
        )

        // Join all labels to selected metrics
        val q2 = q1
            .join(jobMetricLabels).on(_.id === _.metric_id)
            .join(jobRuns).on(_._1.job_id === _.id)
            .map(x => (x._1._1, x._1._2, x._2))

        Await.result(db.run(q2.result), Duration.Inf)
            // Group by Job
            .groupBy(m => (m._3.namespace, m._3.project, m._3.job, m._3.phase, m._1.name))
            .flatMap { case (group,values) =>
                val (namespace, project, job, phase, metric) = group

                // Extract and attach all labels
                val series = values.map(v => (v._1, v._2)).groupBy(_._1.id)
                    .values
                    .map { m =>
                        val metric = m.head._1
                        val labels = m.map(l => l._2.name -> l._2.value).toMap
                        Measurement(metric.name, metric.job_id.toString, metric.ts.toInstant.atZone(ZoneId.systemDefault()), labels, metric.value)
                    }
                // Group by groupings
                series.groupBy(m => groupings.map(m.labels))
                    .map { case(group,values) =>
                        val labels = groupings.zip(group).toMap
                        MetricSeries(metric, namespace, project, job, Phase.ofString(phase), labels, values.toSeq.sortBy(_.ts.toInstant.toEpochMilli))
                    }
            }
            .toSeq
    }


    override def findDocumentation(query: DocumentationQuery): Seq[documentation.EntityDoc] = {
        ???
    }

    private def cmpOpt[T](l:Option[T],r:Option[T])(lt:(T,T) => Int) : Int = {
        if (l.isEmpty && r.isEmpty)
            0
        else if (l.isEmpty)
            -1
        else if (r.isEmpty)
            1
        else
            lt(l.get, r.get)
    }
    private def cmpStr(l:String, r:String) : Int = {
        if (l < r)
            -1
        else if (l == r)
            0
        else
            1
    }
    private def cmpDt(l:ZonedDateTime, r:ZonedDateTime) : Int = {
        if (l == r)
            0
        else if (l.isBefore(r))
            -1
        else
            1
    }

    private def hashMap(map: Map[String, String]): String = {
        val strArgs = map.map(kv => kv._1 + "=" + kv._2).mkString(",")
        val bytes = strArgs.getBytes("UTF-8")
        val digest = MessageDigest.getInstance("MD5").digest(bytes)
        DatatypeConverter.printHexBinary(digest).toUpperCase()
    }

    private def ensureJobExists(jobId:String) : JobRun = {
        val jq = jobRuns.filter(_.id === jobId.toLong)
        Await.result(db.run(jq.result), Duration.Inf).headOption match {
            case Some(state) => state
            case None => throw new NoSuchElementException(s"Job run with id $jobId does not exist")

        }
    }

    private def ensureTargetExists(targetId:String) : TargetRun = {
        val q = targetRuns.filter(r => r.id === targetId.toLong)
        Await.result(db.run(q.result), Duration.Inf).headOption match {
            case Some(state) => state
            case None => throw new NoSuchElementException(s"Target run with id $targetId does not exist")
        }
    }
}
