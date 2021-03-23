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

import com.dimajix.common.IdentityHashMap
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.model.Mapping
import com.dimajix.flowman.model.MappingIdentifier
import com.dimajix.flowman.model.Relation
import com.dimajix.flowman.model.RelationIdentifier
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier


class GraphBuilder(context:Context) {
    private val mappings:IdentityHashMap[Mapping,MappingRef] = IdentityHashMap()
    private val relations:IdentityHashMap[Relation,RelationRef] = IdentityHashMap()
    private val targets:IdentityHashMap[Target,TargetRef] = IdentityHashMap()
    private var currentId:Int = 1

    /**
     * Adds a single [[Mapping]] to the [[GraphBuilder]] and performs all required linking operations to connect the
     * mapping to its inputs
     * @param mapping
     * @return
     */
    def addMapping(mapping:MappingIdentifier) : GraphBuilder = {
        val instance = context.getMapping(mapping)
        refMapping(instance)
        this
    }
    def addMapping(mapping:Mapping) : GraphBuilder = {
        refMapping(mapping)
        this
    }
    def addMappings(mappings:Iterable[MappingIdentifier]) : GraphBuilder = {
        mappings.foreach(addMapping)
        this
    }

    /**
     * Adds a single [[Target]] to the [[GraphBuilder]] and performs all required linking operations to connect the
     * target to its inputs and outputs
     * @param target
     * @return
     */
    def addTarget(target:TargetIdentifier) : GraphBuilder = {
        val instance = context.getTarget(target)
        refTarget(instance)
        this
    }
    def addTarget(target:Target) : GraphBuilder = {
        refTarget(target)
        this
    }
    def addTargets(targets:Iterable[TargetIdentifier]) : GraphBuilder = {
        targets.foreach(addTarget)
        this
    }

    /**
     * Adds a single [[Relation]] to the [[GraphBuilder]] and performs all linking operations.
     * @param relation
     * @return
     */
    def addRelation(relation:RelationIdentifier) : GraphBuilder = {
        val instance = context.getRelation(relation)
        refRelation(instance)
        this
    }
    def addRelation(relation:Relation) : GraphBuilder = {
        refRelation(relation)
        this
    }
    def addRelations(relations:Iterable[RelationIdentifier]) : GraphBuilder = {
        relations.foreach(addRelation)
        this
    }


    /**
     * Retrieves a reference node for a mapping.
     * @param mapping
     * @return
     */
    def refMapping(mapping: Mapping) : MappingRef = {
        val result = mappings.get(mapping)
        if (result.nonEmpty) {
            result.get
        }
        else {
            // Create new node and *first* put it into map of known mappings
            val node = MappingRef(nextId(), mapping)
            mappings.put(mapping, node)
            // Now recursively run the linking process on the newly created node
            val linker = Linker(this, mapping.context, node)
            mapping.link(linker)
            node
        }
    }

    /**
     * Retrieves a reference node for a relation.
     * @param relation
     * @return
     */
    def refRelation(relation: Relation) : RelationRef = {
        val result = relations.get(relation)
        if (result.nonEmpty) {
            result.get
        }
        else {
            // Create new node and *first* put it into map of known relations
            val node = RelationRef(nextId(), relation)
            relations.put(relation, node)
            // Now recursively run the linking process on the newly created node
            val linker = Linker(this, relation.context, node)
            relation.link(linker)
            node
        }
    }

    /**
     * Retrieves a reference node for a target.
     * @param target
     * @return
     */
    def refTarget(target: Target) : TargetRef = {
        val result = targets.get(target)
        if (result.nonEmpty) {
            result.get
        }
        else {
            // Create new node and *first* put it into map of known targets
            val node = TargetRef(nextId(), target)
            targets.put(target, node)
            // Now recursively run the linking process on the newly created node
            val linker = Linker(this, target.context, node)
            target.link(linker)
            node
        }
    }

    /**
     * Builds the full graph
     * @return
     */
    def build() : Graph = Graph(
        context,
        mappings.values.toList,
        relations.values.toList,
        targets.values.toList
    )

    private def nextId() : Int = {
        val result = currentId
        currentId += 1
        result
    }
}
