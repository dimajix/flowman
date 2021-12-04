/*
 * Copyright 2019-2021 Kaya Kupferschmidt
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

import com.dimajix.flowman.history
import com.dimajix.flowman.model


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
            ms.phase,
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

    def ofSpec(resource:history.Resource) : Resource = {
        Resource(resource.category, resource.name, resource.partition)
    }

    def ofSpec(node:history.Node) : Node = {
        Node(
            node.id,
            node.category.lower,
            node.kind,
            node.name,
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
            state.error
        )
    }
}
