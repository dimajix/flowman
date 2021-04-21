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

package com.dimajix.flowman.kernel.rest

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.pathEndOrSingleSlash
import akka.http.scaladsl.server.Directives.pathPrefix
import io.swagger.annotations.Api
import javax.ws.rs.Path

import com.dimajix.flowman.kernel.service.SessionService


@Api(value = "/session/{session}/mapping", produces = "application/json", consumes = "application/json")
@Path("/session/{session}/mapping")
class MappingEndpoint {
    def routes(session:SessionService) : server.Route = pathPrefix("mapping") {(
        pathEndOrSingleSlash {(
            ???
        )}
    )}
}
