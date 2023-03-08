/*
 * Copyright (C) 2021 The Flowman Authors
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

package com.dimajix.flowman.hub.service

import java.net.URL
import java.util.UUID
import java.util.concurrent.TimeUnit

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.ActorSystem
import org.slf4j.LoggerFactory


final class KernelManager(implicit system:ActorSystem) {
    implicit private val ec = system.dispatcher
    private val logger = LoggerFactory.getLogger(classOf[KernelManager])
    private val kernels = mutable.ListBuffer[KernelService]()

    system.scheduler.schedule(
        FiniteDuration(10, TimeUnit.SECONDS),
        FiniteDuration(10, TimeUnit.SECONDS)) {
        // Find all IDs of all terminated kernels
        val terminatedKernels = list().filter(_.state == KernelState.TERMINATED).map(_.id).toSet

        // Clean up terminated kernels
        kernels.synchronized {
            val index = kernels.indexWhere(k => terminatedKernels.contains(k.id))
            if (index >= 0) {
                kernels.remove(index)
            }
        }
    }

    /**
     * Lists all known Kernels
     * @return
     */
    def list() : Seq[KernelService] = {
        val result = mutable.ListBuffer[KernelService]()
        kernels.synchronized {
            result.append(kernels:_*)
        }
        result
    }

    def getKernel(id:String) : Option[KernelService] = {
        var result:Option[KernelService] = None
        kernels.synchronized {
            result = kernels.find(_.id == id)
        }
        result
    }

    def launchKernel(launcher:Launcher, env:LaunchEnvironment) : KernelService = {
        val id = UUID.randomUUID().toString
        val secret = UUID.randomUUID().toString
        val finalEnv = env.copy(id=id, secret=secret)

        // Launch kernel process
        val process = launcher.launch(finalEnv)
        val svc = new KernelService(id, secret, process)(system)

        kernels.synchronized {
            kernels.append(svc)
        }

        svc
    }

    /**
     * This method is indirectly called by a kernel (via the Studio REST interface) in order to register its URL
     * for further communications
     * @param id
     * @param url
     */
    def registerKernel(id:String, url:URL) : Unit = {
        val svc = getKernel(id)
        svc match {
            case Some(svc) =>
                logger.info(s"Register known kernel $id at $url")
                svc.setUrl(url)
            case None =>
                logger.info(s"Register unknown kernel $id at $url")
        }
    }

    def unregisterKernel(kernel:KernelService) : Unit = {
        val id = kernel.id
        kernels.synchronized {
            val index = kernels.indexWhere(_.id == id)
            if (index >= 0) {
                kernels.remove(index)
            }
        }
    }
}
