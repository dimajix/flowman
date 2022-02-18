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

package com.dimajix.flowman.graph

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.NoSuchMappingException
import com.dimajix.flowman.execution.NoSuchRelationException
import com.dimajix.flowman.execution.NoSuchTargetException
import com.dimajix.flowman.execution.Phase
import com.dimajix.flowman.execution.Session
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier


object Graph {
    def empty(context:Context) : Graph = {
        Graph(context, Seq.empty, Seq.empty, Seq.empty)
    }

    /**
     * Creates a Graph from a given project. The [[Context]] required for lookups and instantiation is retrieved from
     * the given [[Session]]
     * @param session
     * @param project
     * @return
     */
    def ofProject(session:Session, project:Project, phase:Phase) : Graph = {
        ofProject(session.getContext(project), project, phase)
    }

    /**
     * Creates a Graph from a given project. The specified [[Context]] has to be created for the given project
     * @param session
     * @param project
     * @return
     */
    def ofProject(context:Context, project:Project, phase:Phase) : Graph = {
        if (context.project.exists(_ ne project))
            throw new IllegalArgumentException("Graph.ofProject requires Context to belong to the given Project")

        val builder = new GraphBuilder(context, phase)
        project.mappings.keys.foreach { name =>
            builder.addMapping(MappingIdentifier(name))
        }
        project.relations.keys.foreach { name =>
            builder.addRelation(RelationIdentifier(name))
        }
        project.targets.keys.foreach { name =>
            builder.addTarget(TargetIdentifier(name))
        }

        builder.build()
    }
}


final case class Graph(
    context:Context,
    mappings:Seq[MappingRef],
    relations:Seq[RelationRef],
    targets:Seq[TargetRef]
) {
    def project : Option[Project] = context.project

    def nodes : Seq[Node] = mappings ++ relations ++ targets
    def edges : Seq[Edge] = nodes.flatMap(_.outgoing)

    /**
     * Tries to retrieve a node representing the specified Mapping
     * @param mapping
     * @return
     */
    def mapping(instance:Mapping) : MappingRef = {
        mappings.find(_.mapping eq instance).getOrElse(throw new NoSuchMappingException(instance.identifier))
    }
    def mapping(id:MappingIdentifier) : MappingRef = {
        val instance = context.getMapping(id)
        mapping(instance)
    }

    /**
     * Tries to retrieve a node representing the specified Relation
     * @param relation
     * @return
     */
    def relation(instance:Relation) : RelationRef = {
        relations.find(_.relation eq instance).getOrElse(throw new NoSuchRelationException(instance.identifier))
    }
    def relation(id:RelationIdentifier) : RelationRef = {
        val instance = context.getRelation(id)
        relation(instance)
    }

    /**
     * Tries to retrieve a node representing the specified Target
     * @param target
     * @return
     */
    def target(instance:Target) : TargetRef = {
        targets.find(_.target eq instance).getOrElse(throw new NoSuchTargetException(instance.identifier))
    }
    def target(id:TargetIdentifier) : TargetRef = {
        val instance = context.getTarget(id)
        target(instance)
    }
}
