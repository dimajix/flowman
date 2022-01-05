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

package com.dimajix.flowman.studio.rest

import akka.http.scaladsl.server
import io.swagger.annotations.Api
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.POST
import javax.ws.rs.Path

import com.dimajix.flowman.studio.model.Status


@Api(value = "/shutdown", produces = "application/json", consumes = "text/plain")
@Path("/shutdown")
class ShutdownEndpoint(fn: => Unit) {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes : server.Route = pathPrefix("shutdown") {
        pathEnd {
            shutdown()
        }
    }

    @POST
    @Path("/")
    @ApiOperation(value = "Shutdown kernel", nickname = "shutdown", httpMethod = "POST")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Ping", response = classOf[String])
    ))
    def shutdown() : server.Route = {
        post {
            entity(as[String]) { body =>
                fn
                complete(Status("success"))
            }
        }
    }
}
