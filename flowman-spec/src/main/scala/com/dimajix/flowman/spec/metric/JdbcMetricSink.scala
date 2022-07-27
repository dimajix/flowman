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

import java.sql.SQLRecoverableException
import java.sql.SQLTransientException

import com.fasterxml.jackson.annotation.JsonProperty
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.jdbc.SlickUtils
import com.dimajix.flowman.metric.AbstractMetricSink
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSink
import com.dimajix.flowman.model.Connection
import com.dimajix.flowman.model.Reference
import com.dimajix.flowman.spec.connection.ConnectionReferenceSpec
import com.dimajix.flowman.spec.connection.JdbcConnection


class JdbcMetricSink(
    connection: Reference[Connection],
    labels: Map[String,String] = Map(),
    tablePrefix: String = "flowman_"
) extends AbstractMetricSink {
    private val logger = LoggerFactory.getLogger(getClass)
    private val retries:Int = 3
    private val timeout:Int = 1000

    override def commit(board:MetricBoard, status:Status): Unit = {
        logger.info(s"Committing execution metrics to JDBC at '${jdbcConnection.url}'")
        val rawLabels = this.labels
        val labels = rawLabels.map { case(k,v) => k -> board.context.evaluate(v, Map("status" -> status.toString)) }

        val metrics = board.metrics(catalog(board), status).collect {
            case metric:GaugeMetric => metric
        }

        withRepository { session =>
            session.commit(metrics, labels)
        }
    }

    /**
     * Performs some a task with a JDBC session, also automatically performing retries and timeouts
     *
     * @param query
     * @tparam T
     * @return
     */
    private def withRepository[T](query: JdbcMetricRepository => T) : T = {
        def retry[T](n:Int)(fn: => T) : T = {
            try {
                fn
            } catch {
                case e @(_:SQLRecoverableException|_:SQLTransientException) if n > 1 => {
                    logger.warn("Retrying after error while executing SQL: {}", e.getMessage)
                    Thread.sleep(timeout)
                    retry(n - 1)(fn)
                }
            }
        }

        retry(retries) {
            ensureTables()
            query(repository)
        }
    }

    private lazy val jdbcConnection = connection.value.asInstanceOf[JdbcConnection]
    private lazy val repository = new JdbcMetricRepository(
        jdbcConnection,
        SlickUtils.getProfile(jdbcConnection.driver),
        tablePrefix
    )

    private var tablesCreated:Boolean = false
    private def ensureTables() : Unit = {
        // Create Database if not exists
        if (!tablesCreated) {
            repository.create()
            tablesCreated = true
        }
    }
}


class JdbcMetricSinkSpec extends MetricSinkSpec {
    @JsonProperty(value = "connection", required = true) private var connection:ConnectionReferenceSpec = _
    @JsonProperty(value = "labels", required = false) private var labels:Map[String,String] = Map.empty
    @JsonProperty(value = "tablePrefix", required = false) private var tablePrefix:String = "flowman_"

    override def instantiate(context: Context, properties:Option[MetricSink.Properties] = None): MetricSink = {
        new JdbcMetricSink(
            connection.instantiate(context),
            labels,
            context.evaluate(tablePrefix)
        )
    }
}
