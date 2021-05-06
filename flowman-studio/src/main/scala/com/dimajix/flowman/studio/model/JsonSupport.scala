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

package com.dimajix.flowman.studio.model

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.DefaultJsonProtocol
import spray.json.RootJsonFormat


trait JsonSupport extends DefaultJsonProtocol with SprayJsonSupport {
    implicit val statusFormat: RootJsonFormat[Status] = jsonFormat1(Status)
    implicit val kernelRegistrationRequestFormat: RootJsonFormat[KernelRegistrationRequest] = jsonFormat2(KernelRegistrationRequest)
    implicit val kernelFormat: RootJsonFormat[Kernel] = jsonFormat1(Kernel)
    implicit val kernelListFormat: RootJsonFormat[KernelList] = jsonFormat1(KernelList)
    implicit val launcherFormat: RootJsonFormat[Launcher] = jsonFormat2(Launcher)
    implicit val launcherListFormat: RootJsonFormat[LauncherList] = jsonFormat1(LauncherList)
}

object JsonSupport extends JsonSupport {
}
