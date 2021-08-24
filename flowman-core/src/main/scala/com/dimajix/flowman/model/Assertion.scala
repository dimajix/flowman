package com.dimajix.flowman.model

import org.apache.spark.sql.DataFrame

import com.dimajix.flowman.execution.Context
import com.dimajix.flowman.execution.Execution


case class AssertionTestResult(
    name:String,
    valid:Boolean,
    exception:Boolean=false
)

case class AssertionResult(
    name:String,
    description:Option[String],
    assertions:Seq[AssertionTestResult]
) {
    def success : Boolean = assertions.forall(_.valid)
    def failure : Boolean = assertions.exists(a => !a.valid)

    def numFailures : Int = assertions.count(a => !a.valid)
    def numSuccesses : Int = assertions.count(_.valid)
    def numExceptions : Int = assertions.count(_.exception)
}


object Assertion {
    object Properties {
        def apply(context: Context, name:String = "", kind:String = "") : Properties = {
            Properties(
                context,
                context.namespace,
                context.project,
                name,
                kind,
                Map(),
                None
            )
        }
    }

    final case class Properties(
        context:Context,
        namespace:Option[Namespace],
        project:Option[Project],
        name:String,
        kind:String,
        labels:Map[String,String],
        description:Option[String]
    ) extends Instance.Properties[Properties] {
        override def withName(name: String): Properties = copy(name=name)
    }
}


trait Assertion extends Instance {
    override def category: String = "assertion"

    /**
     * Returns a description of the assertion
     * @return
     */
    def description : Option[String]

    /**
     * Returns a list of physical resources required by this assertion. This list will only be non-empty for assertions
     * which actually read from physical data.
     * @return
     */
    def requires : Set[ResourceIdentifier]

    /**
     * Returns the dependencies (i.e. names of tables in the Dataflow model)
     * @return
     */
    def inputs : Seq[MappingOutputIdentifier]

    /**
     * Executes this [[Assertion]] and returns a corresponding DataFrame
     *
     * @param execution
     * @param input
     * @return
     */
    def execute(execution:Execution, input:Map[MappingOutputIdentifier,DataFrame]) : Seq[AssertionTestResult]
}


abstract class BaseAssertion extends AbstractInstance with Assertion {
    protected override def instanceProperties : Assertion.Properties

    override def description: Option[String] = instanceProperties.description
}
