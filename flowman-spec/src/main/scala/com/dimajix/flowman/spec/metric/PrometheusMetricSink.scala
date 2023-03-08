/*
 * Copyright (C) 2019 The Flowman Authors
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

import java.io.IOException
import java.net.URI

import scala.util.control.NonFatal

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.http.HttpResponse
import org.apache.http.client.HttpResponseException
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.slf4j.LoggerFactory

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Status
import com.dimajix.flowman.metric.AbstractMetricSink
import com.dimajix.flowman.metric.GaugeMetric
import com.dimajix.flowman.metric.MetricBoard
import com.dimajix.flowman.metric.MetricSink


class PrometheusMetricSink(
    url:String,
    labels:Map[String,String]
)
extends AbstractMetricSink {
    private val logger = LoggerFactory.getLogger(classOf[PrometheusMetricSink])

    override def commit(board:MetricBoard, status:Status) : Unit = {
        val rawLabels = Seq(
            "job" -> this.labels.getOrElse("job","flowman"),
            "instance" -> this.labels.getOrElse("instance", "default")
        ) ++ (this.labels - "job" - "instance").toSeq
        val labels = rawLabels.map(l => l._1 -> board.context.evaluate(l._2, Map("status" -> status.toString)))
        val path = labels.map(kv => kv._1 + "/" + kv._2).mkString("/")
        val url = new URI(this.url).resolve("/metrics/" + path)

        logger.info(s"Committing metrics to Prometheus at '$url'")

        /*
          # TYPE some_metric counter
          some_metric{label="val1"} 42
          # TYPE another_metric gauge
          # HELP another_metric Just an example.
          another_metric 2398.283
        */
        val metrics = board.metrics(catalog(board), status).flatMap { metric =>
            val name = metric.name
            val labels = metric.labels.map(kv => s"""${kv._1}="${sanitize(kv._2)}"""").mkString("{", ",", "}")
            metric match {
                case gauge: GaugeMetric => Some(name -> s"$name$labels ${gauge.value}")
                case _ => None
            }
        }
        val payload = metrics.groupBy(_._1).map { case (name,values) =>
            s"# TYPE $name gauge" + values.map(_._2).mkString("\n","\n","\n")

        }.mkString("\n")

        logger.debug(s"Sending $payload")

        val handler = new AnyRef with ResponseHandler[Unit] {
            @throws[IOException]
            def handleResponse(response: HttpResponse) : Unit  = {
                val statusLine = response.getStatusLine
                if (statusLine.getStatusCode >= 300) {
                    throw new HttpResponseException(statusLine.getStatusCode, statusLine.getReasonPhrase)
                }
            }
        }

        val httpClient = HttpClients.createDefault()
        try {
            val httpPost = new HttpPut(url)
            httpPost.setEntity(new StringEntity(payload))
            httpClient.execute(httpPost, handler)
        }
        catch {
            case ex:HttpResponseException =>
                logger.warn(s"Got error response ${ex.getStatusCode} from Prometheus at '$url': ${ex.getMessage}. Payload was:\n$payload")
            case NonFatal(ex) =>
                logger.warn(s"Error while publishing metrics to Prometheus at '$url': ${ex.getMessage}")
        }
        finally {
            httpClient.close()
        }
    }

    private def sanitize(str:String) : String = {
        str.replace("\"","\\\"").replace("\n","").trim
    }
}


class PrometheusMetricSinkSpec extends MetricSinkSpec {
    @JsonProperty(value = "url", required = true) private var url:String = ""
    @JsonProperty(value = "labels", required = false) private var labels:Map[String,String] = Map()

    override def instantiate(context: Context, properties:Option[MetricSink.Properties] = None): MetricSink = {
        new PrometheusMetricSink(
            context.evaluate(url),
            labels
        )
    }
}
