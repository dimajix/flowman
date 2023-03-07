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

package com.dimajix.flowman.kernel.grpc;

import scala.collection.JavaConverters._

import io.grpc.stub.StreamObserver

import com.dimajix.flowman.FLOWMAN_VERSION
import com.dimajix.flowman.HADOOP_BUILD_VERSION
import com.dimajix.flowman.JAVA_VERSION
import com.dimajix.flowman.SCALA_BUILD_VERSION
import com.dimajix.flowman.SCALA_VERSION
import com.dimajix.flowman.SPARK_BUILD_VERSION
import com.dimajix.flowman.common.ToolConfig
import com.dimajix.flowman.grpc.GrpcService
import com.dimajix.flowman.kernel.RpcUtils.respondTo
import com.dimajix.flowman.kernel.proto.LogEvent
import com.dimajix.flowman.kernel.proto.kernel.GetKernelRequest
import com.dimajix.flowman.kernel.proto.kernel.GetKernelResponse
import com.dimajix.flowman.kernel.proto.kernel.GetNamespaceRequest
import com.dimajix.flowman.kernel.proto.kernel.GetNamespaceResponse
import com.dimajix.flowman.kernel.proto.kernel.KernelDetails
import com.dimajix.flowman.kernel.proto.kernel.KernelServiceGrpc
import com.dimajix.flowman.kernel.proto.kernel.NamespaceDetails
import com.dimajix.flowman.kernel.proto.kernel.ShutdownRequest
import com.dimajix.flowman.kernel.proto.kernel.ShutdownResponse
import com.dimajix.flowman.kernel.proto.kernel.SubscribeLogRequest
import com.dimajix.flowman.kernel.service.SessionManager
import com.dimajix.flowman.plugin.PluginManager
import com.dimajix.hadoop.HADOOP_VERSION
import com.dimajix.spark.SPARK_VERSION


final class KernelServiceHandler(sessionManager: SessionManager, pluginManager: PluginManager, shutdownHook: () => Unit)
    extends KernelServiceGrpc.KernelServiceImplBase with GrpcService {
    private val impl = new KernelServiceHandlerImpl(sessionManager, pluginManager, shutdownHook)
    /**
     */
    override def getNamespace(request: GetNamespaceRequest, responseObserver: StreamObserver[GetNamespaceResponse]): Unit = {
        respondTo(responseObserver) {
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

    /**
     */
    override def getKernel(request: GetKernelRequest, responseObserver: StreamObserver[GetKernelResponse]): Unit = {
        respondTo(responseObserver) {
            val details = KernelDetails.newBuilder()
            ToolConfig.homeDirectory.map(_.toString).foreach(details.setFlowmanHomeDirectory)
            ToolConfig.confDirectory.map(_.toString).foreach(details.setFlowmanConfigDirectory)
            ToolConfig.pluginDirectory.map(_.toString).foreach(details.setFlowmanPluginDirectory)
            details.setFlowmanVersion(FLOWMAN_VERSION)
            details.setSparkVersion(SPARK_VERSION)
            details.setHadoopVersion(HADOOP_VERSION)
            details.setJavaVersion(JAVA_VERSION)
            details.setScalaVersion(SCALA_VERSION)
            details.setSparkBuildVersion(SPARK_BUILD_VERSION)
            details.setHadoopBuildVersion(HADOOP_BUILD_VERSION)
            details.setScalaBuildVersion(SCALA_BUILD_VERSION)
            details.addAllActivePlugins(pluginManager.plugins.keys.asJava)
            details.build()

            GetKernelResponse.newBuilder()
                .setKernel(details)
                .build()
        }
    }

    /**
     */
    override def shutdown(request: ShutdownRequest, responseObserver: StreamObserver[ShutdownResponse]): Unit = {
        respondTo(responseObserver) {
            shutdownHook()
            ShutdownResponse.newBuilder().build()
        }
    }

    /**
     */
    override def subscribeLog(request: SubscribeLogRequest, responseObserver: StreamObserver[LogEvent]): Unit = super.subscribeLog(request, responseObserver)
}
