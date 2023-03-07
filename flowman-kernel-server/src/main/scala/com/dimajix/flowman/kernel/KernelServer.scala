/*
 * Copyright 2018-2023 Kaya Kupferschmidt
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

package com.dimajix.flowman.kernel

import java.net.SocketAddress

import scala.collection.JavaConverters._

import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.fs.File
import com.dimajix.flowman.grpc.GrpcServer
import com.dimajix.flowman.grpc.GrpcService
import com.dimajix.flowman.grpc.InProcessGrpcServer
import com.dimajix.flowman.grpc.NettyGrpcServer
import com.dimajix.flowman.kernel.grpc.KernelServiceHandler
import com.dimajix.flowman.kernel.grpc.SessionServiceHandler
import com.dimajix.flowman.kernel.grpc.WorkspaceServiceHandler
import com.dimajix.flowman.kernel.service.SessionManager
import com.dimajix.flowman.kernel.service.SimpleWorkspaceManager
import com.dimajix.flowman.kernel.service.WorkspaceManager
import com.dimajix.flowman.plugin.PluginManager
import com.dimajix.flowman.spec.storage.SimpleWorkspace
import com.dimajix.flowman.storage.Workspace


object KernelServer {
    class Builder private[kernel](session:Session, pluginManager: PluginManager) {
        require(session != null)
        require(pluginManager != null)

        private var workspaceManager: WorkspaceManager = null
        private var serverName = "flowman-kernel"
        private var port:Int = 80
        private var serverFactory: Iterable[GrpcService] => GrpcServer = createInprocessServer

        private def createInprocessServer(services:Iterable[GrpcService]) : GrpcServer = {
            new InProcessGrpcServer(serverName, services.asJava)
        }
        private def createNettyServer(services:Iterable[GrpcService]) : GrpcServer = {
            new NettyGrpcServer(port, services.asJava)
        }

        def withWorkspaceManager(workspaceManager: WorkspaceManager): Builder = {
            this.workspaceManager = workspaceManager
            this
        }

        def withWorkspace(workspace:Workspace) : Builder = {
            this.workspaceManager = new SimpleWorkspaceManager(workspace)
            this
        }

        def withWorkspaceRoot(workspaceRoot:File) : Builder = {
            val ws = SimpleWorkspace.create(workspaceRoot)
            this.workspaceManager = new SimpleWorkspaceManager(ws)
            this
        }

        def withServerName(name:String) : Builder = {
            this.serverName = name
            this
        }

        def withPort(port:Int) : Builder = {
            this.port = port
            this
        }

        def withInprocessServer() : Builder = {
            serverFactory = createInprocessServer
            this
        }

        def withSocketServer(): Builder = {
            serverFactory = createNettyServer
            this
        }

        def build() : KernelServer = {
            val sessionManager = new SessionManager(session)
            new KernelServer(sessionManager, workspaceManager, pluginManager, serverFactory)
        }
    }

    def builder(session: Session, pluginManager: PluginManager) : Builder = new Builder(session, pluginManager)
}


class KernelServer private(
    val sessionManager: SessionManager,
    val workspaceManager: WorkspaceManager,
    pluginManager: PluginManager,
    serverFactory: Iterable[GrpcService] => GrpcServer
) {
    require(sessionManager != null)
    require(pluginManager != null)

    private val kernelService = new KernelServiceHandler(sessionManager, pluginManager, stop)
    private val sessionService = new SessionServiceHandler(sessionManager, workspaceManager)
    private val workspaceService = new WorkspaceServiceHandler(workspaceManager)

    private val server = serverFactory(Seq(
        kernelService,
        workspaceService,
        sessionService
    ))

    def start() : Unit = server.start()

    def stop() : Unit = server.stop()

    def awaitTermination() : Unit = server.awaitTermination()

    def isTerminated() : Boolean = server.isTerminated

    def getListenSockets(): Seq[SocketAddress] = server.getListenSockets.asScala
}
