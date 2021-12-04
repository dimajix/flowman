/*
 * Copyright 2019-2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.server.model

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.json.DeserializationException
import spray.json.JsString
import spray.json.JsValue
import spray.json.JsonFormat
import spray.json.RootJsonFormat


trait JsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
    implicit object ZonedDateTimeFormat extends JsonFormat[ZonedDateTime] {
        final val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        def write(value:ZonedDateTime) : JsString = {
            JsString(value.format(formatter))
        }
        def read(value:JsValue) : ZonedDateTime = {
            value match {
                case JsString(dt) => ZonedDateTime.parse(dt, formatter)
                case _ => throw DeserializationException("Not a boolean")
            }
        }
    }

    implicit val namespaceFormat: RootJsonFormat[Namespace] = jsonFormat6(Namespace)
    implicit val measurementFormat: RootJsonFormat[Measurement] = jsonFormat5(Measurement)
    implicit val metricSeriesFormat: RootJsonFormat[MetricSeries] = jsonFormat7(MetricSeries)
    implicit val metricSeriesListFormat: RootJsonFormat[MetricSeriesList] = jsonFormat1(MetricSeriesList)
    implicit val jobEnvironmentFormat: RootJsonFormat[JobEnvironment] = jsonFormat1(JobEnvironment)
    implicit val jobStateFormat: RootJsonFormat[JobState] = jsonFormat12(JobState)
    implicit val jobStateListFormat: RootJsonFormat[JobStateList] = jsonFormat2(JobStateList)
    implicit val jobStateCountsFormat: RootJsonFormat[JobStateCounts] = jsonFormat1(JobStateCounts)
    implicit val targetStateFormat: RootJsonFormat[TargetState] = jsonFormat12(TargetState)
    implicit val targetStateListFormat: RootJsonFormat[TargetStateList] = jsonFormat2(TargetStateList)
    implicit val targetStateCountsFormat: RootJsonFormat[TargetStateCounts] = jsonFormat1(TargetStateCounts)
    implicit val resourceFormat: RootJsonFormat[Resource] = jsonFormat3(Resource)
    implicit val nodeFormat: RootJsonFormat[Node] = jsonFormat6(Node)
    implicit val edgeFormat: RootJsonFormat[Edge] = jsonFormat4(Edge)
    implicit val graphFormat: RootJsonFormat[Graph] = jsonFormat2(Graph)
}


object JsonSupport extends JsonSupport {
}
