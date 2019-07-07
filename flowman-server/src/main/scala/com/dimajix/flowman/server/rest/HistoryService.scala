package com.dimajix.flowman.server.rest

import akka.http.scaladsl.server.Route

import com.dimajix.flowman.history.StateStore


class HistoryService(history:StateStore) {
    import akka.http.scaladsl.server.Directives._

    def routes : Route = pathPrefix("history") { reject }
}
