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

package com.dimajix.flowman.spec.relation

import com.fasterxml.jackson.annotation.JsonProperty

import com.dimajix.flowman.config.FlowmanConf
import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.MigrationPolicy
import com.dimajix.flowman.execution.MigrationStrategy


trait MigratableRelationSpec { this: RelationSpec =>
    @JsonProperty(value = "migrationStrategy", required = false) protected var migrationStrategy: Option[String] = None
    @JsonProperty(value = "migrationPolicy", required = false) protected var migrationPolicy: Option[String] = None

    protected def evalMigrationPolicy(context:Context) : MigrationPolicy =
        MigrationPolicy.ofString(
            context.evaluate(migrationPolicy)
                .getOrElse(context.flowmanConf.getConf(FlowmanConf.DEFAULT_RELATION_MIGRATION_POLICY))
        )

    protected def evalMigrationStrategy(context:Context) : MigrationStrategy =
        MigrationStrategy.ofString(
            context.evaluate(migrationStrategy)
                .getOrElse(context.flowmanConf.getConf(FlowmanConf.DEFAULT_RELATION_MIGRATION_STRATEGY))
        )
}
