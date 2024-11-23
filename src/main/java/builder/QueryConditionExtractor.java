package builder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import physicaloperator.base.BooleanExpressionEvaluator;

/**
 * Class to process the WHERE clause of the SQL statement and separate join conditions from filter
 * conditions. It uses the visitor pattern to traverse and classify each comparison expression.
 */
public class QueryConditionExtractor extends ExpressionVisitorAdapter<Void> {
  private final Map<String, Integer> tableOrder = new HashMap<>();
  private final Map<String, Expression> joinConditions = new HashMap<>();
  private final Map<String, Expression> filterConditions = new HashMap<>();
  private boolean alwaysFalseCondition = false;

  /**
   * Constructor to initialize the table processing order and conditions.
   *
   * @param fromItem the main table in the FROM clause
   * @param joins the list of JOIN clauses in the query
   */
  public QueryConditionExtractor(FromItem fromItem, List<Join> joins) {
    initializeTable((Table) fromItem, 0); // Initialize the main table

    int index = 1;
    for (Join join : joins) {
      Table joinTable = (Table) join.getRightItem();
      initializeTable(joinTable, index++);
    }
  }

  /**
   * Initializes the table's join and filter conditions.
   *
   * @param table the table to be initialized
   * @param index the position of the table in the query
   */
  private void initializeTable(Table table, int index) {
    String tableKey = getTableKey(table);
    tableOrder.put(tableKey, index);
    joinConditions.put(tableKey, null);
    filterConditions.put(tableKey, null);
  }

  private String getTableKey(Table table) {
    return (table.getAlias() != null) ? table.getAlias().getName() : table.getName();
  }

  public Expression getJoinConditionsByTable(Table table) {
    return joinConditions.get(getTableKey(table));
  }

  public Expression getFilterConditionsByTable(Table table) {
    return filterConditions.get(getTableKey(table));
  }

  public boolean isAlwaysFalseCondition() {
    return alwaysFalseCondition;
  }

  @Override
  public <S> Void visit(AndExpression andExpression, S context) {
    andExpression.getLeftExpression().accept(this, context);
    andExpression.getRightExpression().accept(this, context);
    return null;
  }

  @Override
  public <S> Void visit(EqualsTo equalsTo, S context) {
    classifyExpression(equalsTo);
    return null;
  }

  @Override
  public <S> Void visit(NotEqualsTo notEqualsTo, S context) {
    classifyExpression(notEqualsTo);
    return null;
  }

  @Override
  public <S> Void visit(GreaterThan greaterThan, S context) {
    classifyExpression(greaterThan);
    return null;
  }

  @Override
  public <S> Void visit(GreaterThanEquals greaterThanEquals, S context) {
    classifyExpression(greaterThanEquals);
    return null;
  }

  @Override
  public <S> Void visit(MinorThan minorThan, S context) {
    classifyExpression(minorThan);
    return null;
  }

  @Override
  public <S> Void visit(MinorThanEquals minorThanEquals, S context) {
    classifyExpression(minorThanEquals);
    return null;
  }

  private void classifyExpression(ComparisonOperator comparison) {
    Expression leftExpr = comparison.getLeftExpression();
    Expression rightExpr = comparison.getRightExpression();

    if (leftExpr instanceof Column leftColumn && rightExpr instanceof Column rightColumn) {
      // If both sides are columns, check their tables
      String leftTableKey = getTableKey(leftColumn.getTable());
      String rightTableKey = getTableKey(rightColumn.getTable());

      if (!leftTableKey.equals(rightTableKey)) {
        // Columns are from different tables, handle as join condition
        handleJoinCondition(comparison, leftTableKey, rightTableKey);
      } else {
        // Columns are from the same table, handle as filter condition
        handleFilterCondition(comparison, leftTableKey);
      }
    } else if (leftExpr instanceof Column leftColumn) {
      // If the left is a Column

      String leftTableKey = getTableKey(leftColumn.getTable());
      handleFilterCondition(comparison, leftTableKey);
    } else if (rightExpr instanceof Column rightColumn) {
      // if the right is a Column

      String rightTableKey = getTableKey(rightColumn.getTable());
      handleFilterCondition(comparison, rightTableKey);
    } else {
      // if non is a Column, we can evaulate it now
      BooleanExpressionEvaluator booleanExpressionEvaluator = new BooleanExpressionEvaluator();
      Boolean isTrue = comparison.accept(booleanExpressionEvaluator, null);
      if (!isTrue) {
        this.alwaysFalseCondition = true;
      }
    }
  }

  private void handleJoinCondition(
      Expression comparison, String leftTableKey, String rightTableKey) {
    if (tableOrder.get(leftTableKey) > tableOrder.get(rightTableKey)) {
      joinConditions.merge(
          leftTableKey, comparison, (existing, newExpr) -> new AndExpression(existing, newExpr));
    } else {
      joinConditions.merge(
          rightTableKey, comparison, (existing, newExpr) -> new AndExpression(existing, newExpr));
    }
  }

  private void handleFilterCondition(Expression comparison, String tableKey) {
    filterConditions.merge(
        tableKey, comparison, (existing, newExpr) -> new AndExpression(existing, newExpr));
  }
}
