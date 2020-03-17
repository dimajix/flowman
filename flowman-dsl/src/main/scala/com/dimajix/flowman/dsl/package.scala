package com.dimajix.flowman

package object dsl {
    type RelationList = WrapperList[model.Relation,RelationWrapper]
    type TargetList = WrapperList[model.Target,TargetWrapper]
    type MappingList = WrapperList[model.Mapping,MappingWrapper]
    type JobList = WrapperList[model.Job,JobWrapper]
}
