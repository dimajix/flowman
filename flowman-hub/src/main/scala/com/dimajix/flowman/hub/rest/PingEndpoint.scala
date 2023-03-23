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

package com.dimajix.flowman.hub.rest

import akka.http.scaladsl.server
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.POST
import javax.ws.rs.Path

import com.dimajix.flowman.hub.model.Status


@Api(value = "ping", produces = "application/json", consumes = "application/json")
@Path("/ping")
class PingEndpoint {
    import akka.http.scaladsl.server.Directives._
    import com.dimajix.flowman.hub.model.JsonSupport._

    def routes : server.Route = pathPrefix("ping") {
        pathEnd {
            ping()
        }
    }

    @POST
    @Path("")
    @ApiOperation(value = "Ping", nickname = "ping", httpMethod = "POST")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "ping message", value = "some message to ping", required = true,
            dataTypeClass = classOf[String], paramType = "body")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Ping", response = classOf[String])
    ))
    private def ping() : server.Route = {
        post {
            entity(as[String]) { body =>
                complete(Status("success"))
            }
        }
    }
}
