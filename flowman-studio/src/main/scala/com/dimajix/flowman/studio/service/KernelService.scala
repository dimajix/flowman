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

package com.dimajix.flowman.studio.service

import java.net.URL
import java.util.UUID

import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse


abstract class KernelState
object KernelState {
    object STARTING extends KernelState
    object RUNNING extends KernelState
    object STOPPING extends KernelState
    object TERMINATED extends KernelState
}

class KernelService(val process: Process) {
    val id : String = UUID.randomUUID().toString
    val secret : String = UUID.randomUUID().toString

    def url : URL = ???

    def state : KernelState = ???

    def shutdown() : Unit = ???

    def invoke(request:HttpRequest) : HttpResponse = ???
}
