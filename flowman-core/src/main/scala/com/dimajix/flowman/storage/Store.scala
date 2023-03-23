/*
 * Copyright (C) 2018 The Flowman Authors
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

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model
import com.dimajix.flowman.model.AbstractInstance
import com.dimajix.flowman.model.Category
import com.dimajix.flowman.model.Instance
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.Project


object Store {
    final case class Properties(
        kind:String
    ) extends model.Properties[Properties] {
        override val context : Context = null
        override val namespace : Option[Namespace] = None
        override val project : Option[Project] = None
        override val name : String = ""
        override val metadata : Metadata = Metadata(name="", category=model.Category.PROJECT_STORE.lower, kind=kind)

        override def withName(name: String): Properties = ???
    }
}

/**
 * The [[Store]] is the abstract class for implementing project stores. These stores offer an abstraction of
 * persistent storage, which allows projects to be stored not only in filesystems, but also in databases. To
 * enable this flexibility, projects are references solely by their name and not by their physical storage location
 * like a path, filename or directory.
 */
trait Store extends Instance {
    override type PropertiesType = Store.Properties

    /**
     * Returns the category of the resource
     *
     * @return
     */
    final override def category: Category = Category.PROJECT_STORE

    /**
     * Loads a project via its name (not its filename or directory)
     * @param name
     * @return
     */
    def loadProject(name:String) : Project

    /**
     * Retrieves a list of all projects. The returned projects only contain some fundamental information
     * like the project's name, its basedir and so on. The project itself (mappings, relations, targets etc)
     * will not be loaded
     * @return
     */
    def listProjects() : Seq[Project]
}


abstract class AbstractStore extends AbstractInstance with Store {
    override protected def instanceProperties: Store.Properties = Store.Properties(kind)
}
