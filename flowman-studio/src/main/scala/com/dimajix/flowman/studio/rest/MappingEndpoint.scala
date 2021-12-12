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

package com.dimajix.flowman.studio.rest

import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import io.swagger.annotations.Api
import io.swagger.annotations.ApiImplicitParam
import io.swagger.annotations.ApiImplicitParams
import io.swagger.annotations.ApiOperation
import io.swagger.annotations.ApiParam
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.GET
import javax.ws.rs.Path

import com.dimajix.flowman.model
import com.dimajix.flowman.studio.model.Converter
import com.dimajix.flowman.studio.model.Mapping
import com.dimajix.flowman.studio.model.Mapping
import com.dimajix.flowman.studio.model.MappingList
import com.dimajix.flowman.studio.model.MappingList
import com.dimajix.flowman.studio.service.SessionService


@Api(value = "/session/{session}/mapping", produces = "application/json", consumes = "application/json")
@Path("/session/{session}/mapping")
@ApiImplicitParams(Array(
    new ApiImplicitParam(name = "session", value = "Session ID", required = true, dataType = "string", paramType = "path")
))
@ApiResponses(Array(
    new ApiResponse(code = 404, message = "Session or mapping not found"),
    new ApiResponse(code = 500, message = "Internal server error")
))
class MappingEndpoint {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(session:SessionService) : server.Route = pathPrefix("mapping") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listMappings(session)
            }
        }
        ~
        pathPrefix(Segment) { mappingName => (
            pathEndOrSingleSlash {
                redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                    getMapping(session, mappingName)
                }
            })
        }
    )}

    @GET
    @ApiOperation(value = "Return list of all jobs", nickname = "listMappings", httpMethod = "GET")
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "List of all mappings", response = classOf[MappingList])
    ))
    def listMappings(@ApiParam(hidden = true) session: SessionService) : server.Route = {
        get {
            val result = MappingList(
                session.listMappings()
            )
            complete(result)
        }
    }

    @GET
    @Path("/{mapping}")
    @ApiOperation(value = "Get mapping", nickname = "getMapping", httpMethod = "GET")
    @ApiImplicitParams(Array(
        new ApiImplicitParam(name = "mapping", value = "Mapping Name", required = true, dataType = "string", paramType = "path")
    ))
    @ApiResponses(Array(
        new ApiResponse(code = 200, message = "Information about the mapping", response = classOf[Mapping])
    ))
    def getMapping(@ApiParam(hidden = true) session: SessionService, @ApiParam(hidden = true) mapping:String) : server.Route = {
        get {
            withMapping(session, mapping) { mapping =>
                complete(Converter.of(mapping))
            }
        }
    }

    private def withMapping(session:SessionService, mappingName:String)(fn:(model.Mapping) => server.Route) : server.Route = {
        Try {
            session.getMapping(mappingName)
        } match {
            case Success(mapping) => fn(mapping)
            case Failure(_) => complete(HttpResponse(status = StatusCodes.NotFound))
        }
    }
}
