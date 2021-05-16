/*
 * Copyright 2018 Kaya Kupferschmidt
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

package com.dimajix.flowman.storage

import com.dimajix.flowman.model.Project


/**
 * The [[Store]] is the abstract class for implementing project stores. These stores offer an abstraction of
 * persistent storage, which allows projects to be stored not only in filesystems, but also in databases. To
 * enable this flexibility, projects are references solely by their name and not by their physical storage location
 * like a path, filename or directory.
 */
abstract class Store {
    /**
     * Loads a project via its name (not its filename or directory)
     * @param name
     * @return
     */
    def loadProject(name:String) : Project

    /**
     * Stores a project inside this persistent storage
     * @param project
     */
    def storeProject(project: Project) : Unit

    /**
     * Removes a project from this persistent storage
     * @param name
     */
    def removeProject(name:String) : Unit

    /**
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the projects's name, its basedir and so on. The project itself (mappings, relations, targets etc)
     * will not be loaded
     * @return
     */
    def listProjects() : Seq[Project]
}
