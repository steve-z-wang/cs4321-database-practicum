package common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import operator.JoinOperator;
import operator.Operator;
import operator.ProjectOperator;
import operator.ScanOperator;
import operator.SelectOperator;

/**
 * Class to translate a JSQLParser statement into a relational algebra query plan. For now only
 * works for Statements that are Selects, and specifically PlainSelects. Could implement the visitor
 * pattern on the statement, but doesn't for simplicity as we do not handle nesting or other complex
 * query features.
 *
 * <p>Query plan fixes join order to the order found in the from clause and uses a left deep tree
 * join. Maximally pushes selections on individual relations and evaluates join conditions as early
 * as possible in the join tree. Projections (if any) are not pushed and evaluated in a single
 * projection operator after the last join. Finally, sorting and duplicate elimination are added if
 * needed.
 *
 * <p>For the subset of SQL which is supported as well as assumptions on semantics, see the Project
 * 2 student instructions, Section 2.1
 */
public class QueryPlanBuilder {
  public QueryPlanBuilder() {}

  /**
   * Top level method to translate statement to query plan
   *
   * @param stmt statement to be translated
   * @return the root of the query plan
   * @precondition stmt is a Select having a body that is a PlainSelect
   */
  @SuppressWarnings("unchecked")
  public Operator buildPlan(Statement stmt) throws ExecutionControl.NotImplementedException {

    PlainSelect plainSelect = (PlainSelect) (Select) stmt;

    // Extract parts
    List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
    FromItem fromItem = plainSelect.getFromItem();
    List<Join> Joins = plainSelect.getJoins();
    Expression where = plainSelect.getWhere();

    // Create the initial operator for fromItem
    Table mainTable = (Table) fromItem;
    Operator operator = new ScanOperator(mainTable);

    if (Joins == null) {
      // If there are no joins, create a select operator for the where clause
      if (where != null) {
        operator = new SelectOperator(operator, where);
      }
    } else {

      // Process where clause
      WhereClauseProcessor whereClauseProcessor = new WhereClauseProcessor(fromItem, Joins);
      where.accept(whereClauseProcessor, null);

      // Filter conditions for the main table
      Expression filterCondition =
          whereClauseProcessor.getFilterConditionsByTable(mainTable.getName());
      if (filterCondition != null) {
        operator = new SelectOperator(operator, filterCondition);
      }

      Operator previousOperator = operator;
      Table previousTable = mainTable;
      for (Join join : Joins) {
        Table joinTable = (Table) join.getRightItem();

        // Create a scan operator for each table
        operator = new ScanOperator(joinTable);

        // Push down filter conditions
        filterCondition = whereClauseProcessor.getFilterConditionsByTable(joinTable.getName());
        if (filterCondition != null) {
          operator = new SelectOperator(operator, filterCondition);
        }

        // Add join conditions
        Expression joinCondition =
            whereClauseProcessor.getJoinConditionsByTable(previousTable.getName());
        operator = new JoinOperator(previousOperator, operator, joinCondition);

        previousOperator = operator;
      }
    }

    // Create a project operator if not selecting all columns
    if (!(selectItems.get(0).getExpression() instanceof AllColumns)) {

      ArrayList<Column> projectSchema = new ArrayList<>();
      for (SelectItem<?> selectItem : selectItems) {
        projectSchema.add((Column) selectItem.getExpression());
      }

      operator = new ProjectOperator(operator, projectSchema);
    }

    return operator;
  }
}

class WhereClauseProcessor extends ExpressionVisitorAdapter<Object> {
  private final Map<String, Integer> tableOrder = new HashMap<>();
  private final Map<String, Expression> joinConditions = new HashMap<>();
  private final Map<String, Expression> filterConditions = new HashMap<>();

  public WhereClauseProcessor(FromItem fromItem, List<Join> joins) {
    String mainTableName = ((Table) fromItem).getName();
    tableOrder.put(mainTableName, 0);
    joinConditions.put(mainTableName, null);
    filterConditions.put(mainTableName, null);

    int index = 1;
    for (Join join : joins) {
      String joinTableName = ((Table) join.getRightItem()).getName();
      tableOrder.put(joinTableName, index++);
      joinConditions.put(joinTableName, null);
      filterConditions.put(joinTableName, null);
    }
  }

  public Expression getJoinConditionsByTable(String tableName) {
    return joinConditions.get(tableName);
  }

  public Expression getFilterConditionsByTable(String tableName) {
    return filterConditions.get(tableName);
  }

  @Override
  public <S> Object visit(AndExpression andExpression, S context) {
    andExpression.getLeftExpression().accept(this, context);
    andExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public <S> Object visit(EqualsTo equalsTo, S context) {
    classifyExpression(equalsTo);
    return null;
  }

  @Override
  public <S> Object visit(NotEqualsTo notEqualsTo, S context) {
    classifyExpression(notEqualsTo);
    return null;
  }

  @Override
  public <S> Object visit(GreaterThan greaterThan, S context) {
    classifyExpression(greaterThan);
    return null;
  }

  @Override
  public <S> Object visit(GreaterThanEquals greaterThanEquals, S context) {
    classifyExpression(greaterThanEquals);
    return null;
  }

  @Override
  public <S> Object visit(MinorThan minorThan, S context) {
    classifyExpression(minorThan);
    return null;
  }

  @Override
  public <S> Object visit(MinorThanEquals minorThanEquals, S context) {
    classifyExpression(minorThanEquals);
    return null;
  }

  private void classifyExpression(ComparisonOperator comparison) {
    Expression leftExpr = comparison.getLeftExpression();
    Expression rightExpr = comparison.getRightExpression();

    if (leftExpr instanceof Column leftColumn && rightExpr instanceof Column rightColumn) {
      // If both sides are columns, check their tables
      String leftTable = leftColumn.getTable().getName();
      String rightTable = rightColumn.getTable().getName();

      if (!leftTable.equals(rightTable)) {
        // Columns are from different tables, handle as join condition
        handleJoinCondition(comparison, leftColumn, rightColumn);
      } else {
        // Columns are from the same table, handle as filter condition
        handleFilterCondition(comparison, leftColumn);
      }
    } else if (leftExpr instanceof Column leftColumn) {
      handleFilterCondition(comparison, leftColumn);
    } else if (rightExpr instanceof Column rightColumn) {
      handleFilterCondition(comparison, rightColumn);
    }
  }

  private void handleJoinCondition(Expression comparison, Column leftColumn, Column rightColumn) {
    String leftTable = leftColumn.getTable().getName();
    String rightTable = rightColumn.getTable().getName();

    if (tableOrder.get(leftTable) < tableOrder.get(rightTable)) {
      joinConditions.merge(
          leftTable, comparison, (existing, newExpr) -> new AndExpression(existing, newExpr));
    } else {
      joinConditions.merge(
          rightTable, comparison, (existing, newExpr) -> new AndExpression(existing, newExpr));
    }
  }

  private void handleFilterCondition(Expression comparison, Column column) {
    String tableName = column.getTable().getName();
    filterConditions.merge(
        tableName, comparison, (existing, newExpr) -> new AndExpression(existing, newExpr));
  }
}
