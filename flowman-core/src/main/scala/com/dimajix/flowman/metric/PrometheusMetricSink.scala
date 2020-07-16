/*
 * Copyright 2019 Kaya Kupferschmidt
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

package com.dimajix.flowman.metric

import java.io.IOException
import java.net.URI

import scala.util.control.NonFatal

import org.apache.http.HttpResponse
import org.apache.http.client.HttpResponseException
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.slf4j.LoggerFactory


class PrometheusMetricSink(
    url:String,
    labels:Map[String,String]
)
extends AbstractMetricSink {
    private val logger = LoggerFactory.getLogger(classOf[PrometheusMetricSink])

    override def commit(board:MetricBoard) : Unit = {
        val labels = Seq(
            "job" -> this.labels.getOrElse("job","flowman"),
            "instance" -> this.labels.getOrElse("instance", "default")
        ) ++ (this.labels - "job" - "instance").toSeq
        val path = labels.map(kv => kv._1 + "/" + kv._2).mkString("/")
        val url = new URI(this.url).resolve("/metrics/" + path)
        logger.info(s"Publishing all metrics to Prometheus at $url")

        /*
          # TYPE some_metric counter
          some_metric{label="val1"} 42
          # TYPE another_metric gauge
          # HELP another_metric Just an example.
          another_metric 2398.283
        */

        implicit val catalog = this.catalog(board)
        val payload = board.selections.map { selection =>
            val name = selection.name
            val metrics = selection.metrics.map { metric =>
                val labels = metric.labels.map(kv => s"""${kv._1}="${kv._2}"""").mkString("{",",","}")
                metric match {
                    case gauge:GaugeMetric => s"$name$labels ${gauge.value}"
                    case _ => ""
                }
            }
            s"# TYPE $name gauge" + metrics.mkString("\n","\n","\n")
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
            case NonFatal(ex) =>
                logger.warn(s"Cannot publishing metrics to Prometheus at $url", ex)
        }
        finally {
            httpClient.close()
        }
    }
}
