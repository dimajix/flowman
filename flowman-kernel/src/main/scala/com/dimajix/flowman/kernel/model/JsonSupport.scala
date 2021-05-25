/*
 * Copyright 2021 Kaya Kupferschmidt
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

package com.dimajix.flowman.kernel.model

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

    implicit val kernelRegistrationRequestFormat: RootJsonFormat[KernelRegistrationRequest] = jsonFormat2(KernelRegistrationRequest)
    implicit val statusFormat: RootJsonFormat[Status] = jsonFormat1(Status)
    implicit val namespaceFormat: RootJsonFormat[Namespace] = jsonFormat6(Namespace)
    implicit val projectFormat: RootJsonFormat[Project] = jsonFormat11(Project)
    implicit val projectListFormat: RootJsonFormat[ProjectList] = jsonFormat1(ProjectList)
    implicit val jobFormat: RootJsonFormat[Job] = jsonFormat5(Job)
    implicit val jobListFormat: RootJsonFormat[JobList] = jsonFormat1(JobList)
    implicit val jobTaskFormat: RootJsonFormat[JobTask] = jsonFormat9(JobTask)
    implicit val runJobRequestFormat: RootJsonFormat[RunJobRequest] = jsonFormat6(RunJobRequest)
    implicit val testFormat: RootJsonFormat[Test] = jsonFormat3(Test)
    implicit val testListFormat: RootJsonFormat[TestList] = jsonFormat1(TestList)
    implicit val targetFormat: RootJsonFormat[Target] = jsonFormat5(Target)
    implicit val targetListFormat: RootJsonFormat[TargetList] = jsonFormat1(TargetList)
    implicit val mappingFormat: RootJsonFormat[Mapping] = jsonFormat8(Mapping)
    implicit val mappingListFormat: RootJsonFormat[MappingList] = jsonFormat1(MappingList)
    implicit val relationFormat: RootJsonFormat[Relation] = jsonFormat3(Relation)
    implicit val relationListFormat: RootJsonFormat[RelationList] = jsonFormat1(RelationList)
    implicit val sessionFormat: RootJsonFormat[Session] = jsonFormat5(Session)
    implicit val createSessionRequestFormat: RootJsonFormat[CreateSessionRequest] = jsonFormat2(CreateSessionRequest)
    implicit val sessionListFormat: RootJsonFormat[SessionList] = jsonFormat1(SessionList)
}


object JsonSupport extends JsonSupport {
}
