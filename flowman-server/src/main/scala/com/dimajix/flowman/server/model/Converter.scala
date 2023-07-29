/*
 * Copyright (C) 2019 The Flowman Authors
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

package com.dimajix.flowman.server.model

import java.time.Duration

import com.dimajix.flowman.history
import com.dimajix.flowman.model
import com.dimajix.flowman.documentation


object Converter {
    def ofSpec(ms:history.Measurement) : Measurement = {
        Measurement(
            ms.name,
            ms.jobId,
            ms.ts,
            ms.labels,
            ms.value
        )
    }

    def ofSpec(ms:history.MetricSeries) : MetricSeries = {
        MetricSeries(
            ms.metric,
            ms.namespace,
            ms.project,
            ms.job,
            ms.phase.toString,
            ms.labels,
            ms.measurements.map(m => ofSpec(m))
        )
    }

    def ofSpec(ns:model.Namespace) : Namespace = {
        Namespace(
            ns.name,
            ns.environment,
            ns.config,
            ns.profiles.keys.toSeq,
            ns.connections.keys.toSeq,
            ns.plugins
        )
    }

    def ofSpec(identifier: model.Identifier[_]): Identifier = {
        Identifier(identifier.project, identifier.name)
    }

    def ofSpec(resource: model.ResourceIdentifier): Resource = {
        Resource(resource.category, resource.name, resource.partition)
    }

    def ofSpec(node:history.Node) : Node = {
        Node(
            node.id,
            node.category.lower,
            node.kind,
            node.project,
            node.name,
            ofSpec(node.identifier),
            node.provides.map(r => ofSpec(r)),
            node.requires.map(r => ofSpec(r))
        )
    }

    def ofSpec(edge:history.Edge) : Edge = {
        Edge(
            edge.input.id,
            edge.output.id,
            edge.action.upper,
            edge.labels
        )
    }

    def ofSpec(graph:history.Graph) : Graph = {
        Graph(
            graph.nodes.map(n => ofSpec(n)),
            graph.edges.map(n => ofSpec(n))
        )
    }

    def ofSpec(jobState:history.JobState, measurements:Seq[history.Measurement]=Seq()) : JobState = {
        JobState(
            jobState.id,
            jobState.namespace,
            jobState.project,
            jobState.version,
            jobState.job,
            jobState.phase.toString,
            jobState.args,
            jobState.status.toString,
            jobState.startDateTime,
            jobState.endDateTime,
            jobState.endDateTime.map(dt => Duration.between(jobState.startDateTime.getOrElse(dt), dt)),
            jobState.error,
            measurements.map(ofSpec)
        )
    }

    def ofSpec(state:history.TargetState) : TargetState = {
        TargetState(
            state.id,
            state.jobId,
            state.namespace,
            state.project,
            state.version,
            state.target,
            state.partitions,
            state.phase.toString,
            state.status.toString,
            state.startDateTime,
            state.endDateTime,
            state.endDateTime.map(dt => Duration.between(state.startDateTime.getOrElse(dt), dt)),
            state.error
        )
    }

    def ofSpec(doc:documentation.ProjectDoc) : ProjectDocumentation = {
        ProjectDocumentation(
            doc.name,
            doc.version,
            doc.description,
            doc.relations.map(ofSpec)
        )
    }

    def ofSpec(doc:documentation.RelationDoc) : RelationDocumentation = {
        RelationDocumentation(
            doc.parent.map(_.toString),
            doc.kind,
            ofSpec(doc.identifier),
            doc.description,
            doc.schema.map(ofSpec),
            doc.inputs.map(_.toString),
            doc.provides.map(ofSpec),
            doc.requires.map(ofSpec),
            doc.sources.map(ofSpec)
        )
    }

    def ofSpec(doc:documentation.SchemaDoc) : SchemaDocumentation = {
        SchemaDocumentation(
            doc.parent.map(_.toString),
            doc.description,
            doc.columns.map(ofSpec)
        )
    }

    def ofSpec(doc:documentation.ColumnDoc) : ColumnDocumentation = {
        ColumnDocumentation(
            doc.parent.map(_.toString),
            doc.name,
            doc.fqName,
            doc.typeName,
            doc.sqlType,
            doc.nullable,
            doc.description,
            doc.children.map(ofSpec),
            doc.inputs.map(_.toString),
            doc.index
        )
    }
}
