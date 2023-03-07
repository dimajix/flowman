package com.dimajix.flowman.kernel.grpc

import scala.collection.JavaConverters._

import com.dimajix.flowman.kernel.proto.kernel.GetNamespaceRequest
import com.dimajix.flowman.kernel.proto.kernel.GetNamespaceResponse
import com.dimajix.flowman.kernel.proto.kernel.NamespaceDetails
import com.dimajix.flowman.kernel.service.SessionManager
import com.dimajix.flowman.plugin.PluginManager


class KernelServiceHandlerImpl(sessionManager: SessionManager, pluginManager: PluginManager, shutdownHook: () => Unit) {
    def getNamespace(request: GetNamespaceRequest): GetNamespaceResponse = {
        val details = NamespaceDetails.newBuilder()

        sessionManager.rootSession.namespace.foreach { ns =>
            details.setName(ns.name)
            details.addAllPlugins(ns.plugins.asJava)
            details.putAllEnvironment(ns.environment.asJava)
            details.putAllConfig(ns.config.asJava)
            details.addAllProfiles(ns.profiles.keys.asJava)
            details.addAllConnections(ns.connections.keys.asJava)
        }

        GetNamespaceResponse.newBuilder()
            .setNamespace(details.build())
            .build()
    }

}
