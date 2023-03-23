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

package com.dimajix.flowman

import com.dimajix.flowman.model.Prototype


package object dsl {
    type RelationList = WrapperList[model.Relation,model.Relation.Properties]
    type TargetList = WrapperList[model.Target,model.Target.Properties]
    type MappingList = WrapperList[model.Mapping,model.Mapping.Properties]
    type JobList = WrapperList[model.Job,model.Job.Properties]

    type RelationWrapper = Wrapper[model.Relation, model.Relation.Properties]
    type TargetWrapper = Wrapper[model.Target, model.Target.Properties]
    type MappingWrapper = Wrapper[model.Mapping, model.Mapping.Properties]
    type JobWrapper = Wrapper[model.Job, model.Job.Properties]

    type RelationGen = (model.Relation.Properties => model.Relation)
    type TargetGen = (model.Target.Properties => model.Target)
    type MappingGen = (model.Mapping.Properties => model.Mapping)
    type JobGen = (model.Job.Properties => model.Job)

    type SchemaGen = Prototype[model.Schema]
    type DatasetGen = Prototype[model.Dataset]
}
