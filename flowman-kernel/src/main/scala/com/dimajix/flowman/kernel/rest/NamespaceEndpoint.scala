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
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.Path

import com.dimajix.flowman.kernel.model.Converter
import com.dimajix.flowman.kernel.model.Namespace
import com.dimajix.flowman.model


@Api(value = "/namespace", produces = "application/json", consumes = "application/json")
@Path("/namespace")
class NamespaceEndpoint(ns:model.Namespace) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.kernel.model.JsonSupport._

    def routes : server.Route = pathPrefix("namespace") {
        pathEndOrSingleSlash {
            info()
        }
    }

    @ApiOperation(value = "Return information on the current namespace", nickname = "getNamespace", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about namespace", response = classOf[Namespace])
    ))
    def info() : server.Route = {
        get {
            complete(Converter.of(ns))
        }
    }
}

