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

import scala.collection.mutable


final class LauncherManager {
    private val launchers = mutable.ListBuffer[Launcher]()

    /**
     * Returns a list of all available Launchers
     * @return
     */
    def list() : Seq[Launcher] = {
        val result = mutable.ListBuffer[Launcher]()
        launchers.synchronized {
            result.append(launchers:_*)
        }
        result
    }

    /**
     * Retrieves a specific launcher by its name. If no such launcher is known, [[None]] will be returned instead
     * @param name
     * @return
     */
    def getLauncher(name:String) : Option[Launcher] = {
        var result:Option[Launcher] = None
        launchers.synchronized {
            result = launchers.find(_.name == name)
        }
        result
    }

    /**
     * Registers a new Launcher. If another launcher with the same name is already registered, it will be replaced
     * with the new launcher
     * @param launcher
     */
    def addLauncher(launcher:Launcher) : Unit = {
        launchers.synchronized {
            val name = launcher.name
            val idx = launchers.indexWhere(_.name == name)
            if (idx >= 0)
                launchers.remove(idx)
            launchers.append(launcher)
        }
    }
}
