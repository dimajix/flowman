package com.dimajix.flowman.kernel.rest

import akka.http.scaladsl.server
import akka.http.scaladsl.server.Directives.pathEndOrSingleSlash
import akka.http.scaladsl.server.Directives.pathPrefix

class ProjectEndpoint {
    def routes : server.Route = pathPrefix("project") {(
        pathEndOrSingleSlash {(
            ???
        )}
}
