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

package com.dimajix.flowman.spec.metric

import java.io.IOException
import java.net.URI

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.http.HttpResponse
import org.apache.http.client.HttpResponseException
import org.apache.http.client.ResponseHandler
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.slf4j.LoggerFactory

import com.smartclip.sxp.datatool.execution.Context
import com.smartclip.sxp.datatool.metric.GaugeMetric


class PrometheusMetricSinkSpec extends MetricSinkSpec {
    private val logger = LoggerFactory.getLogger(classOf[PrometheusMetricSinkSpec])

    @JsonProperty(value = "url", required = true) private[spec] var _url:String = _
    @JsonProperty(value = "job", required = true) private[spec] var _job:String = "datatool"
    @JsonProperty(value = "instance", required = true) private[spec] var _instance:String = "default"
    @JsonProperty(value = "labels", required = true) private[spec] var _labels:Map[String,String] = Map()

}
