/*
 * Copyright 2022 Kaya Kupferschmidt
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

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.server.Route
import io.swagger.annotations.Api
import io.swagger.annotations.ApiResponse
import io.swagger.annotations.ApiResponses
import javax.ws.rs.Path

import com.dimajix.flowman.storage.Workspace


@Api(value = "/workspace/{workspace}/parcel", produces = "application/json", consumes = "application/json")
@Path("/workspace/{workspace}/parcel")
@ApiResponses(Array(
    new ApiResponse(code = 500, message = "Internal server error")
))
class ParcelEndpoint {
    import akka.http.scaladsl.server.Directives._

    import com.dimajix.flowman.studio.model.JsonSupport._

    def routes(workspace:Workspace) : Route = pathPrefix("parcel") {(
        pathEndOrSingleSlash {
            redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {
                listParcels(workspace)
            }
        }
            ~
            pathPrefix(Segment) { parcel =>
                pathEndOrSingleSlash {
                    redirectToNoTrailingSlashIfPresent(StatusCodes.Found) {(
                        getParcel(workspace, parcel)
                        ~
                        createParcel(workspace, parcel)
                        ~
                        updateParcel(workspace, parcel)
                        ~
                        deleteParcel(workspace, parcel)
                    )}
                }
            }
        )}

    def listParcels(workspace:Workspace) : server.Route = {
        get {
            ???
        }
    }

    def getParcel(workspace:Workspace, parcel:String) : server.Route = {
        get {
            ???
        }
    }
    def createParcel(workspace:Workspace, parcel:String) : server.Route = {
        post {
            ???
        }
    }
    def updateParcel(workspace:Workspace, parcel:String) : server.Route = {
        put {
            ???
        }
    }
    def deleteParcel(workspace:Workspace, parcel:String) : server.Route = {
        delete {
            ???
        }
    }
}
