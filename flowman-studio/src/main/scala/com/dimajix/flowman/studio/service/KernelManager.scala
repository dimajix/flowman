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

package com.dimajix.flowman.studio.service

import java.net.URL

import scala.collection.mutable


class KernelManager {
    private val kernels = mutable.ListBuffer[KernelService]()

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

    def launchKernel(launcher:Launcher) : KernelService = {
        val process = launcher.launch()
        val svc = new KernelService(process)

        kernels.synchronized {
            kernels.append(svc)
        }

        svc
    }

    def registerKernel(id:String, url:URL) : Unit = {
        val svc = getKernel(id)
        ???
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
