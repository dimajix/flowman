/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.spark.sql.catalyst

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable.Map
import scala.language.implicitConversions
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.CollapseProject
import org.apache.spark.sql.catalyst.optimizer.CombineFilters
import org.apache.spark.sql.catalyst.optimizer.CombineUnions
import org.apache.spark.sql.catalyst.optimizer.PushProjectionThroughUnion
import org.apache.spark.sql.catalyst.optimizer.RemoveRedundantAliases
import org.apache.spark.sql.catalyst.optimizer.SimplifyCasts
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.ByteType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.NullType

import com.dimajix.util.Reflection


object SqlBuilder {
    // Helper method for creating API compatibility for Spark 2.3 and Spark 2.4
    private implicit def optionToSeq[T](o:Option[T]) : Seq[T] = o.toList
}

/**
 * A builder class used to convert a resolved logical plan into a SQL query string.  Note that not
 * all resolved logical plan are convertible.  They either don't have corresponding SQL
 * representations (e.g. logical plans that operate on local Scala collections), or are simply not
 * supported by this builder (yet).
 */
class SqlBuilder private(
    logicalPlan: LogicalPlan,
    nextSubqueryId: AtomicLong,
    nextGenAttrId: AtomicLong,
    exprIdMap: Map[Long, Long]) extends Logging {
  require(logicalPlan.resolved,
    "SQLBuilder only supports resolved logical query plans. Current plan:\n" + logicalPlan)

  import SqlBuilder.optionToSeq

  def this(logicalPlan: LogicalPlan) =
    this(logicalPlan, new AtomicLong(0), new AtomicLong(0), Map.empty[Long, Long])

  def this(df: Dataset[_]) = this(df.queryExecution.analyzed)

  private def newSubqueryName(): String = s"gen_subquery_${nextSubqueryId.getAndIncrement()}"
  private def normalizedName(n: NamedExpression): String = synchronized {
    "gen_attr_" + exprIdMap.getOrElseUpdate(n.exprId.id, nextGenAttrId.getAndIncrement())
  }

  def toSQL: String = {
    val canonicalizedPlan = Canonicalizer.execute(logicalPlan)
    val outputNames = logicalPlan.output.map(_.name)
    val qualifiers = logicalPlan.output.flatMap(_.qualifier).distinct

    // Keep the qualifier information by using it as sub-query name, if there is only one qualifier
    // present.
    val finalName = if (qualifiers.length == 1) {
      qualifiers.head
    } else {
      newSubqueryName()
    }

    // Canonicalizer will remove all naming information, we should add it back by adding an extra
    // Project and alias the outputs.
    val aliasedOutput = canonicalizedPlan.output.zip(outputNames).map {
      case (attr, name) => Alias(attr.withQualifier(None), name)()
    }
    //val finalPlan = Project(aliasedOutput, SubqueryAlias(finalName, canonicalizedPlan))
    val finalPlan = Simplifier.execute(Project(aliasedOutput, canonicalizedPlan))

    try {
      val replaced = finalPlan.transformAllExpressions {
        case s: SubqueryExpression =>
          val query = new SqlBuilder(s.plan, nextSubqueryId, nextGenAttrId, exprIdMap).toSQL
          val sql = s match {
            case _: ListQuery => query
            case _: Exists => s"EXISTS($query)"
            case _ => s"($query)"
          }
          SubqueryHolder(sql)
        case e: NonSQLExpression =>
          throw new UnsupportedOperationException(
            s"Expression $e doesn't have a SQL representation"
          )
        case e => e
      }

      //println("== Original Plan ==")
      //println(logicalPlan.toString())
      //println("== Canonicalized Plan ==")
      //println(canonicalizedPlan.toString())
      //println("== Final Plan ==")
      //println(replaced.toString())

      val generatedSQL = toSQL(replaced)
      logDebug(
        s"""Built SQL query string successfully from given logical plan:
           |
           |== Original logical plan ==
           |${logicalPlan.treeString}
           |== Canonicalized logical plan ==
           |${canonicalizedPlan.treeString}
           |== Final plan ==
           |${replaced.treeString}
           |== Generated SQL ==
           |$generatedSQL
         """.stripMargin)
      generatedSQL
    } catch { case NonFatal(e) =>
      logDebug(
        s"""Failed to build SQL query string from given logical plan:
           |
           |# Original logical plan:
           |${logicalPlan.treeString}
           |# Canonicalized logical plan:
           |${canonicalizedPlan.treeString}
         """.stripMargin)
      throw e
    }
  }

  private def toSQL(node: LogicalPlan): String = node match {
    case Distinct(p: Project) =>
      projectToSQL(p, isDistinct = true)

    case p: Project =>
      projectToSQL(p, isDistinct = false)

    case a @ Aggregate(_, _, e @ Expand(_, _, p: Project)) if isGroupingSet(a, e, p) =>
      groupingSetToSQL(a, e, p)

    case p: Aggregate =>
      aggregateToSQL(p)

    case w: Window =>
      windowToSQL(w)

    case g: Generate =>
      generateToSQL(g)

    // This prevents a pattern of `((...) AS gen_subquery_0 LIMIT 1)` which does not work.
    // For example, `SELECT * FROM (SELECT id FROM tbl TABLESAMPLE (2 ROWS))` makes this plan.
    case Limit(limitExpr, child: SubqueryAlias) =>
      s"${toSQL(child)} LIMIT ${limitExpr.sql}"

    case Limit(limitExpr, child) =>
      s"(${toSQL(child)} LIMIT ${limitExpr.sql})"

    case Filter(condition, child) =>
      val whereOrHaving = child match {
        case _: Aggregate => "HAVING"
        case _ => "WHERE"
      }
      build(toSQL(child), whereOrHaving, condition.sql)

    case p @ Distinct(u: Union) if u.children.length > 1 =>
      val childrenSql = u.children.map(c => s"${toSQL(c)}")
      childrenSql.mkString(" UNION DISTINCT ")

    case p: Union if p.children.length > 1 =>
      val childrenSql = p.children.map(c => s"${toSQL(c)}")
      childrenSql.mkString(" UNION ALL ")

    case p: Intersect =>
      build("(" + toSQL(p.left), ") INTERSECT (", toSQL(p.right) + ")")

    case p: Except =>
      build("(" + toSQL(p.left), ") EXCEPT (", toSQL(p.right) + ")")

    case p: SubqueryAlias => build("(" + toSQL(p.child) + ")", "AS", p.alias)

    case p: Join =>
      build(
        toSQL(p.left),
        p.joinType.sql,
        "JOIN",
        toSQL(p.right),
        p.condition.map(" ON " + _.sql).getOrElse(""))

    case SQLTable(database, table, _, sample) =>
      val qualifiedName = s"${quoteIdentifier(database)}.${quoteIdentifier(table)}"
      sample.map { case (lowerBound, upperBound) =>
        val fraction = math.min(100, math.max(0, (upperBound - lowerBound) * 100))
        qualifiedName + " TABLESAMPLE(" + fraction + " PERCENT)"
      }.getOrElse(qualifiedName)

    case relation: HiveTableRelation =>
      val m = relation.tableMeta
      val qualifiedName = s"${quoteIdentifier(m.database)}.${quoteIdentifier(m.identifier.table)}"
      qualifiedName

    case view: View =>
        val m = view.desc
        val qualifiedName = s"${quoteIdentifier(m.database)}.${quoteIdentifier(m.identifier.table)}"
        qualifiedName

    case Sort(orders, _, RepartitionByExpression(partitionExprs, child, _))
        if orders.map(_.child) == partitionExprs =>
      build(toSQL(child), "CLUSTER BY", partitionExprs.map(_.sql).mkString(", "))

    case p: Sort =>
      build(
        toSQL(p.child),
        if (p.global) "ORDER BY" else "SORT BY",
        p.order.map(SqlExpressionBuilder.sortOrderToSsql).mkString(", ")
      )

    case p: RepartitionByExpression =>
      build(
        toSQL(p.child),
        "DISTRIBUTE BY",
        p.partitionExpressions.map(_.sql).mkString(", ")
      )

    case p: ScriptTransformation =>
      scriptTransformationToSQL(p)

    case p: LocalRelation =>
      p.toSQL(newSubqueryName())

    case p: Range =>
      p.toSQL()

    case _: OneRowRelation =>
      ""

    case _ =>
      throw new UnsupportedOperationException(s"unsupported plan $node")
  }

  /**
   * Turns a bunch of string segments into a single string and separate each segment by a space.
   * The segments are trimmed so only a single space appears in the separation.
   * For example, `build("a", " b ", " c")` becomes "a b c".
   */
  private def build(segments: String*): String =
    segments.map(_.trim).filter(_.nonEmpty).mkString(" ")

  private def projectToSQL(plan: Project, isDistinct: Boolean): String = {
    build(
      "SELECT",
      if (isDistinct) "DISTINCT" else "",
      plan.projectList.map(SqlExpressionBuilder.toSql).mkString(", "),
      plan.child match { case _:OneRowRelation => ""; case _ => "FROM" },
      toSQL(plan.child)
    )
  }

  private def scriptTransformationToSQL(plan: ScriptTransformation): String = {
    val inputRowFormatSQL = plan.ioschema.inputRowFormatSQL.getOrElse(
      throw new UnsupportedOperationException(
        s"unsupported row format ${plan.ioschema.inputRowFormat}"))
    val outputRowFormatSQL = plan.ioschema.outputRowFormatSQL.getOrElse(
      throw new UnsupportedOperationException(
        s"unsupported row format ${plan.ioschema.outputRowFormat}"))

    val outputSchema = plan.output.map { attr =>
      s"${attr.sql} ${attr.dataType.simpleString}"
    }.mkString(", ")

    build(
      "SELECT TRANSFORM",
      "(" + plan.child.output.map(_.sql).mkString(", ") + ")",
      inputRowFormatSQL,
      s"USING \'${plan.script}\'",
      "AS (" + outputSchema + ")",
      outputRowFormatSQL,
      plan.child match { case _:OneRowRelation => ""; case _ => "FROM" },
      toSQL(plan.child)
    )
  }

  private def aggregateToSQL(plan: Aggregate): String = {
    val groupingSQL = plan.groupingExpressions.map(_.sql).mkString(", ")
    build(
      "SELECT",
      plan.aggregateExpressions.map(_.sql).mkString(", "),
      plan.child match { case _:OneRowRelation => ""; case _ => "FROM" },
      toSQL(plan.child),
      if (groupingSQL.isEmpty) "" else "GROUP BY",
      groupingSQL
    )
  }

  private def generateToSQL(g: Generate): String = {
    val columnAliases = g.generatorOutput.map(_.sql).mkString(", ")

    val childSQL = g.child match {
        case _: OneRowRelation =>
          // This only happens when we put UDTF in project list and there is no FROM clause. Because we
          // always generate LATERAL VIEW for `Generate`, here we use a trick to put a dummy sub-query
          // after FROM clause, so that we can generate a valid LATERAL VIEW SQL string.
          // For example, if the original SQL is: "SELECT EXPLODE(ARRAY(1, 2))", we will convert in to
          // LATERAL VIEW format, and generate:
          // SELECT col FROM (SELECT 1) sub_q0 LATERAL VIEW EXPLODE(ARRAY(1, 2)) sub_q1 AS col
          s"(SELECT 1) ${newSubqueryName()}"
        case _ => toSQL(g.child)
    }

    // The final SQL string for Generate contains 7 parts:
    //   1. the SQL of child, can be a table or sub-query
    //   2. the LATERAL VIEW keyword
    //   3. an optional OUTER keyword
    //   4. the SQL of generator, e.g. EXPLODE(array_col)
    //   5. the table alias for output columns of generator.
    //   6. the AS keyword
    //   7. the column alias, can be more than one, e.g. AS key, value
    // A concrete example: "tbl LATERAL VIEW EXPLODE(map_col) sub_q AS key, value", and the builder
    // will put it in FROM clause later.
    build(
      childSQL,
      "LATERAL VIEW",
      if (g.outer) "OUTER" else "",
      g.generator.sql,
      newSubqueryName(),
      "AS",
      columnAliases
    )
  }

  private def sameOutput(output1: Seq[Attribute], output2: Seq[Attribute]): Boolean =
    output1.size == output2.size &&
      output1.zip(output2).forall(pair => pair._1.semanticEquals(pair._2))

  private def isGroupingSet(a: Aggregate, e: Expand, p: Project): Boolean = {
    assert(a.child == e && e.child == p)
    a.groupingExpressions.forall(_.isInstanceOf[Attribute]) && sameOutput(
      e.output.drop(p.child.output.length),
      a.groupingExpressions.map(_.asInstanceOf[Attribute]))
  }

  private def groupingSetToSQL(agg: Aggregate, expand: Expand, project: Project): String = {
    assert(agg.groupingExpressions.length > 1)

    // The last column of Expand is always grouping ID
    val gid = expand.output.last

    val numOriginalOutput = project.child.output.length
    // Assumption: Aggregate's groupingExpressions is composed of
    // 1) the grouping attributes
    // 2) gid, which is always the last one
    val groupByAttributes = agg.groupingExpressions.dropRight(1).map(_.asInstanceOf[Attribute])
    // Assumption: Project's projectList is composed of
    // 1) the original output (Project's child.output),
    // 2) the aliased group by expressions.
    val expandedAttributes = project.output.drop(numOriginalOutput)
    val groupByExprs = project.projectList.drop(numOriginalOutput).map(_.asInstanceOf[Alias].child)
    val groupingSQL = groupByExprs.map(_.sql).mkString(", ")

    // a map from group by attributes to the original group by expressions.
    val groupByAttrMap = AttributeMap(groupByAttributes.zip(groupByExprs))
    // a map from expanded attributes to the original group by expressions.
    val expandedAttrMap = AttributeMap(expandedAttributes.zip(groupByExprs))

    val groupingSet: Seq[Seq[Expression]] = expand.projections.map { project =>
      // Assumption: expand.projections is composed of
      // 1) the original output (Project's child.output),
      // 2) expanded attributes(or null literal)
      // 3) gid, which is always the last one in each project in Expand
      project.drop(numOriginalOutput).dropRight(1).collect {
        case attr: Attribute if expandedAttrMap.contains(attr) => expandedAttrMap(attr)
      }
    }
    val groupingSetSQL = "GROUPING SETS(" +
      groupingSet.map(e => s"(${e.map(_.sql).mkString(", ")})").mkString(", ") + ")"

    val aggExprs = agg.aggregateExpressions.map { case aggExpr =>
      val originalAggExpr = aggExpr.transformDown {
        // grouping_id() is converted to VirtualColumn.groupingIdName by Analyzer. Revert it back.
        case ar: AttributeReference if ar == gid => GroupingID(Nil)
        case ar: AttributeReference if groupByAttrMap.contains(ar) => groupByAttrMap(ar)
        case a @ Cast(BitwiseAnd(
            ShiftRight(ar: AttributeReference, Literal(value: Any, IntegerType)),
            Literal(1, IntegerType)), ByteType, _, _) if ar == gid =>
          // for converting an expression to its original SQL format grouping(col)
          val idx = groupByExprs.length - 1 - value.asInstanceOf[Int]
          groupByExprs.lift(idx).map(Grouping).getOrElse(a)
      }

      originalAggExpr match {
        // Ancestor operators may reference the output of this grouping set, and we use exprId to
        // generate a unique name for each attribute, so we should make sure the transformed
        // aggregate expression won't change the output, i.e. exprId and alias name should remain
        // the same.
        case ne: NamedExpression if ne.exprId == aggExpr.exprId => ne
        case e => Alias(e, normalizedName(aggExpr))(exprId = aggExpr.exprId)
      }
    }

    build(
      "SELECT",
      aggExprs.map(_.sql).mkString(", "),
      agg.child match { case _:OneRowRelation => ""; case _ => "FROM" },
      toSQL(project.child),
      "GROUP BY",
      groupingSQL,
      groupingSetSQL
    )
  }

  private def windowToSQL(w: Window): String = {
    build(
      "SELECT",
      (w.child.output ++ w.windowExpressions).map(_.sql).mkString(", "),
      w.child match { case _:OneRowRelation => ""; case _ => "FROM" },
      toSQL(w.child)
    )
  }

  object Canonicalizer extends RuleExecutor[LogicalPlan] {
    override protected def batches: Seq[Batch] = Seq(
      Batch("Prepare", FixedPoint(100),
        // The `WidenSetOperationTypes` analysis rule may introduce extra `Project`s over
        // `Aggregate`s to perform type casting.  This rule merges these `Project`s into
        // `Aggregate`s.
        CollapseProject,
        // Parser is unable to parse the following query:
        // SELECT  `u_1`.`id`
        // FROM (((SELECT  `t0`.`id` FROM `default`.`t0`)
        // UNION ALL (SELECT  `t0`.`id` FROM `default`.`t0`))
        // UNION ALL (SELECT  `t0`.`id` FROM `default`.`t0`)) AS u_1
        // This rule combine adjacent Unions together so we can generate flat UNION ALL SQL string.
        CombineUnions),
      Batch("Recover Scoping Info", Once,
        // Replace View with a HiveTableRelation to truncate execution plan
        ReplaceView,
        // A logical plan is allowed to have same-name outputs with different qualifiers(e.g. the
        // `Join` operator). However, this kind of plan can't be put under a sub query as we will
        // erase and assign a new qualifier to all outputs and make it impossible to distinguish
        // same-name outputs. This rule renames all attributes, to guarantee different
        // attributes(with different exprId) always have different names. It also removes all
        // qualifiers, as attributes have unique names now and we don't need qualifiers to resolve
        // ambiguity.
        NormalizedAttribute,
        // Insert sub queries on top of operators that need to appear after FROM clause.
        // Our analyzer will add one or more sub-queries above table relation, this rule removes
        // these sub-queries so that next rule can combine adjacent table relation and sample to
        // SQLTable.
        RemoveSubqueriesAboveSQLTable,
        // Finds the table relations and wrap them with `SQLTable`s.  If there are any `Sample`
        // operators on top of a table relation, merge the sample information into `SQLTable` of
        // that table relation, as we can only convert table sample to standard SQL string.
        ResolveSQLTable,
        // Insert sub queries on top of operators that need to appear after FROM clause.
        AddSubquery,
        // Reconstruct subquery expressions.
        ConstructSubqueryExpressions,
        // Normalize union attributes
        NormalizeUnionAttributes
      )
    )

    object NormalizedAttribute extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
        // Do not replace column names in HiveTableRelation, otherwise an assertion will fail
        case relation:HiveTableRelation => relation
        // Also do not replace column names in Views, otherwise an assertion will fail
        case view:View => view
        // Also do not replace column names in other relations
        case relation:LogicalRelation => relation
        case p => normalizeAttributes(p)
      }

      private def normalizeAttributes(plan: LogicalPlan): LogicalPlan = plan.transformExpressions {
        case a: AttributeReference =>
          AttributeReference(normalizedName(a), a.dataType)(exprId = a.exprId, qualifier = None)
        case a: Alias =>
          Alias(a.child, normalizedName(a))(exprId = a.exprId, qualifier = None)
      }
    }

    object NormalizeUnionAttributes extends Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
            case union:Union =>
                union.copy(children = normalizeAttributes(union.children))
        }

        private def normalizeAttributes(children:Seq[LogicalPlan]) : Seq[LogicalPlan] = {
            val headNames = children.head.output.map(_.name)

            val tailPlans = children.tail.map { plan =>
                val aliases = plan.output.zip(headNames).map { case(att,name) => Alias(att, name)() }
                Project(aliases, plan)
            }

            children.head +: tailPlans
        }
    }

    object ReplaceView extends Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
            case view: View =>
                val m = view.desc
                val cols = view.output.map(a => AttributeReference(a.name, a.dataType, a.nullable, a.metadata)(a.exprId, a.qualifier))
                HiveTableRelation(view.desc, cols, Seq())
        }
    }

    object RemoveSubqueriesAboveSQLTable extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
        case SubqueryAlias(_, t @ ExtractSQLTable(_)) => t
      }
    }

    object ResolveSQLTable extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
        case Sample(lowerBound, upperBound, _, _, ExtractSQLTable(table)) =>
          aliasColumns(table.withSample(lowerBound, upperBound))
        case ExtractSQLTable(table) =>
          aliasColumns(table)
      }

      /**
       * Aliases the table columns to the generated attribute names, as we use exprId to generate
       * unique name for each attribute when normalize attributes, and we can't reference table
       * columns with their real names.
       */
      private def aliasColumns(table: SQLTable): LogicalPlan = {
        val aliasedOutput = table.output.map { attr =>
          Alias(attr, normalizedName(attr))(exprId = attr.exprId)
        }
        //addSubquery(Project(aliasedOutput, table))
        Project(aliasedOutput, table)
      }
    }

    object AddSubquery extends Rule[LogicalPlan] {
      override def apply(tree: LogicalPlan): LogicalPlan = tree transformUp {
        // This branch handles aggregate functions within HAVING clauses.  For example:
        //
        //   SELECT key FROM src GROUP BY key HAVING max(value) > "val_255"
        //
        // This kind of query results in query plans of the following form because of analysis rule
        // `ResolveAggregateFunctions`:
        //
        //   Project ...
        //    +- Filter ...
        //        +- Aggregate ...
        //            +- MetastoreRelation default, src, None
        case p @ Project(_, f @ Filter(_, _: Aggregate)) => p.copy(child = addSubquery(f))

        case w @ Window(_, _, _, f @ Filter(_, _: Aggregate)) => w.copy(child = addSubquery(f))

        case s: Sort => s.copy(child = addSubquery(s.child))

        case p: Project => p.copy(child = addSubqueryIfNeeded(p.child))

        // We will generate "SELECT ... FROM ..." for Window operator, so its child operator should
        // be able to put in the FROM clause, or we wrap it with a subquery.
        case w: Window => w.copy(child = addSubqueryIfNeeded(w.child))

        case j: Join => j.copy(
          left = addSubqueryIfNeeded(j.left),
          right = addSubqueryIfNeeded(j.right))

        // A special case for Generate. When we put UDTF in project list, followed by WHERE, e.g.
        // SELECT EXPLODE(arr) FROM tbl WHERE id > 1, the Filter operator will be under Generate
        // operator and we need to add a sub-query between them, as it's not allowed to have a WHERE
        // before LATERAL VIEW, e.g. "... FROM tbl WHERE id > 2 EXPLODE(arr) ..." is illegal.
        case g @ Generate(_, _, _, _, _, f: Filter) =>
          // Add an extra `Project` to make sure we can generate legal SQL string for sub-query,
          // for example, Subquery -> Filter -> Table will generate "(tbl WHERE ...) AS name", which
          // misses the SELECT part.
          val proj = Project(f.output, f)
          g.copy(child = addSubquery(proj))
      }
    }

    object ConstructSubqueryExpressions extends Rule[LogicalPlan] {
      def apply(tree: LogicalPlan): LogicalPlan = tree transformAllExpressions {
        case ScalarSubquery(query, conditions, exprId, joinCond) if conditions.nonEmpty =>
          def rewriteAggregate(a: Aggregate): Aggregate = {
            val filter = Filter(conditions.reduce(And), addSubqueryIfNeeded(a.child))
            Aggregate(Nil, a.aggregateExpressions.take(1), filter)
          }
          val cleaned = query match {
            case Project(_, child) => child
            case child => child
          }
          val rewrite = cleaned match {
            case a: Aggregate =>
              rewriteAggregate(a)
            case Filter(c, a: Aggregate) =>
              Filter(c, rewriteAggregate(a))
          }
          ScalarSubquery(rewrite, Seq.empty, exprId)
/*
        case PredicateSubquery(query, conditions, false, exprId) =>
          val subquery = addSubqueryIfNeeded(query)
          val plan = if (conditions.isEmpty) {
            subquery
          } else {
            Project(Seq(Alias(Literal(1), "1")()),
              Filter(conditions.reduce(And), subquery))
          }
          Exists(plan, exprId)

        case PredicateSubquery(query, conditions, true, exprId) =>
          val (in, correlated) = conditions.partition(_.isInstanceOf[EqualTo])
          val (outer, inner) = in.zipWithIndex.map {
            case (EqualTo(l, r), i) if query.outputSet.intersect(r.references).nonEmpty =>
              (l, Alias(r, s"_c$i")())
            case (EqualTo(r, l), i) =>
              (l, Alias(r, s"_c$i")())
          }.unzip
          val wrapped = addSubqueryIfNeeded(query)
          val filtered = if (correlated.nonEmpty) {
            Filter(conditions.reduce(And), wrapped)
          } else {
            wrapped
          }
          val value = outer match {
            case Seq(expr) => expr
            case exprs => CreateStruct(exprs)
          }
          In(value, Seq(ListQuery(Project(inner, filtered), exprId)))
 */
      }
    }

    private def addSubquery(plan: LogicalPlan): SubqueryAlias = {
      SubqueryAlias(newSubqueryName(), plan)
    }

    private def addSubqueryIfNeeded(plan: LogicalPlan): LogicalPlan = plan match {
      case _: SubqueryAlias => plan
      case _: Filter => plan
      case _: Join => plan
      case _: LocalLimit => plan
      case _: GlobalLimit => plan
      case _: SQLTable => plan
      case _: Generate => plan
      case _: OneRowRelation => plan
      // case _: Union => plan // Simplifying "SELECT ... FROM (... UNION ...)" will be performed via "RemoveSubquery"
      case _ => addSubquery(plan)
    }
  }

  object Simplifier extends RuleExecutor[LogicalPlan] {
    // Figure out the right PushDownPredicate, depending on the Spark version and distribution
    private val PushDownPredicate = {
      Reflection.companion[Rule[LogicalPlan]]("org.apache.spark.sql.catalyst.optimizer.PushPredicateThroughNonJoin")
        .orElse(Reflection.companion[Rule[LogicalPlan]]("org.apache.spark.sql.catalyst.optimizer.PushDownPredicate"))
        .get
    }
    override protected def batches: Seq[Batch] = Seq(
      Batch("Simplify plan", FixedPoint(100),
        // It is a good idea to bring projections as near as possible to the inputs, since this
        // prevents additional subqueries
        PushProjectionThroughUnion,
        // Predicates should be pushed down to the sources as well
        PushDownPredicate,
        // Simplify Sort
        //PullUpSort,
        // Remove subqueries
        RemoveSubqueries
      ),
      Batch("Replace complex operators", Once,
        // Replace window nodes by simple projections again
        ReplaceWindow
      ),
      Batch("Collapse project and filter", FixedPoint(100),
        // It is a good idea to bring projections as near as possible to the inputs, since this
        // prevents additional subqueries
        PushProjectionThroughUnion,
        // Remove subqueries
        RemoveSubqueries,
        // When we now have pushed down everything, try combine stuff
        CombineFilters,
        // When we now have pushed down everything, try combine stuff
        CollapseProject,
        // Remove aliases
        RemoveRedundantAliases,
        // Remove redundant casts
        SimplifyCasts
      )
    )

    object ReplaceWindow  extends Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
            case Window(expressions, _, _, p @ Project(_, child)) =>
                Project(p.projectList ++ expressions, child)

            case Window(expressions, _, _, child) =>
                Project(child.output ++ expressions, child)
        }
    }

    object PullUpSort extends Rule[LogicalPlan] {
        override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
            // Simplify Project - Sort - Project
            // TODO: This is not working
            case p1 @ Project(_, s @ Sort(_, _, p2: Project)) => s.copy(child = p1.copy(child = p2))
        }
    }

    object RemoveSubqueries extends Rule[LogicalPlan] {
      override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
        case Project(_, t @ ExtractSQLTable(_)) => t

        case SubqueryAlias(_, t @ SQLTable(_,_,_,_)) => t

        case SubqueryAlias(_, a2 @ SubqueryAlias(_,_)) => a2

        // Remove subqueries between project and UNION
        case p1 @ Project(_, a @ SubqueryAlias(_, u)) if u.isInstanceOf[Union] => p1.copy(child = u)

        // Move projections together, they will be merged by CollapseProject
        case p1 @ Project(_, a @ SubqueryAlias(_, p2 @ Project(_, c))) => p1.copy(child = p2.copy(child = a.copy(child = c)))

        // Move project and filter together, since this is a natural SELECT ... WHERE
        case p @ Project(_, a @ SubqueryAlias(_, f @ Filter(_, c))) => p.copy(child = f.copy(child = a.copy(child=c)))

        // Pull up project, this will be optimized further by PushDownPredicate
        //case f @ Filter(_, a @ SubqueryAlias(_, p @ Project(_, c))) => f.copy(child = p.copy(child = a.copy(child=c)))
      }
    }
  }

  case class SQLTable(
      database: String,
      table: String,
      output: Seq[Attribute],
      sample: Option[(Double, Double)] = None) extends LeafNode {
    def withSample(lowerBound: Double, upperBound: Double): SQLTable =
      this.copy(sample = Some(lowerBound -> upperBound))
  }

  object ExtractSQLTable {
    def unapply(plan: LogicalPlan): Option[SQLTable] = plan match {
      case l @ LogicalRelation(_, _, Some(catalogTable), _)
          if catalogTable.identifier.database.isDefined =>
        Some(SQLTable(
          catalogTable.identifier.database.get,
          catalogTable.identifier.table,
          l.output.map(_.withQualifier(None))))

      case view: View =>
        val m = view.desc
        Some(SQLTable(m.database, m.identifier.table, view.output.map(_.withQualifier(None))))

      case relation: HiveTableRelation =>
        val m = relation.tableMeta
        Some(SQLTable(m.database, m.identifier.table, relation.output.map(_.withQualifier(None))))

      case _ => None
    }
  }

  /**
   * A place holder for generated SQL for subquery expression.
   */
  case class SubqueryHolder(override val sql: String) extends LeafExpression with Unevaluable {
    override def dataType: DataType = NullType
    override def nullable: Boolean = true
  }
}
