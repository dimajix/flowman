
package org.apache.spark.sql.classic


object DataFrameBridge {
    def ofRows[T](sparkSession: _root_.org.apache.spark.sql.classic.SparkSession, logicalPlan: _root_.org.apache.spark.sql.catalyst.plans.logical.LogicalPlan, encoderGenerator: () => _root_.org.apache.spark.sql.Encoder[T]): _root_.org.apache.spark.sql.classic.Dataset[T] = {
        Dataset(sparkSession, logicalPlan, encoderGenerator)

    }

    def ofRows(sparkSession: _root_.org.apache.spark.sql.classic.SparkSession, logicalPlan: _root_.org.apache.spark.sql.catalyst.plans.logical.LogicalPlan): _root_.org.apache.spark.sql.classic.DataFrame = {
        Dataset.ofRows(sparkSession, logicalPlan)
    }
}
