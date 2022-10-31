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

package com.dimajix.flowman.storage

import com.dimajix.flowman.fs.File
import com.dimajix.flowman.model.Project


trait Workspace extends Store {
    def name : String

    def root : File

    /**
     * Loads a project via its name (not its filename or directory)
     *
     * @param name
     * @return
     */
    override def loadProject(name: String): Project

    /**
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the projects name, its basedir and so on. The project itself (mappings, relations, targets etc)
     * will not be loaded
     *
     * @return
     */
    override def listProjects(): Seq[Project]

    /**
     * Returns the list of all Parcels which belong to this Workspace
     * @return
     */
    def parcels : Seq[Parcel]

    /**
     * Add a new [[Parcel]] to the workspace. Afterwards, the Parcel and all contained Projects should be resolvable
     * via the Workspace.
     * @return
     */
    def addParcel(parcel:Parcel) : Unit

    /**
     * Removes a Parcel from the Workspace again
     * @param parcel
     */
    def removeParcel(parcel:String) : Unit
}


abstract class AbstractWorkspace extends AbstractStore with Workspace {

}
