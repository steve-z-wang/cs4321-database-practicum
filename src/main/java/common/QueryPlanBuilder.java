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
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import operator.BooleanEvaluator;
import operator.DuplicateEliminationOperator;
import operator.EmptyOperator;
import operator.JoinOperator;
import operator.Operator;
import operator.ProjectOperator;
import operator.ScanOperator;
import operator.SelectOperator;
import operator.SortOperator;

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
    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
    Distinct distinct = plainSelect.getDistinct();

    // Create the initial operator for fromItem
    Table mainTable = (Table) fromItem;
    Operator operator = new ScanOperator(mainTable);

    // If there are no joins but a WHERE clause exists, apply selection filtering
    if (Joins == null && where != null) {
      operator = new SelectOperator(operator, where);
    }
    // If joins exist but no WHERE clause, process the joins without filtering
    else if (Joins != null && where == null) {

      Operator previousOperator = operator;
      for (Join join : Joins) {
        Table joinTable = (Table) join.getRightItem();
        operator = new ScanOperator(joinTable);
        operator = new JoinOperator(previousOperator, operator);
        previousOperator = operator;
      }

    }
    // If both joins and a WHERE clause exist, process both accordingly
    else if (Joins != null && where != null) {

      // Process where clause
      WhereClauseProcessor whereClauseProcessor = new WhereClauseProcessor(fromItem, Joins);
      where.accept(whereClauseProcessor, null);

      // Check if the condition is always false
      if (whereClauseProcessor.isAlwaysFalseCondition()) {
        return new EmptyOperator();
      }

      // Filter conditions for the main table
      Expression filterCondition = whereClauseProcessor.getFilterConditionsByTable(mainTable);
      if (filterCondition != null) {
        operator = new SelectOperator(operator, filterCondition);
      }

      // Process joins and apply join conditions where appropriate
      Operator previousOperator = operator;
      Table previousTable = mainTable;
      for (Join join : Joins) {
        Table joinTable = (Table) join.getRightItem();

        // Create a scan operator for each table
        operator = new ScanOperator(joinTable);

        // Apply filter conditions for each join table
        filterCondition = whereClauseProcessor.getFilterConditionsByTable(joinTable);
        if (filterCondition != null) {
          operator = new SelectOperator(operator, filterCondition);
        }

        // Add join conditions to the join operator
        Expression joinCondition = whereClauseProcessor.getJoinConditionsByTable(previousTable);
        operator = new JoinOperator(previousOperator, operator, joinCondition);

        previousOperator = operator;
      }
    }

    // Create a project operator if not selecting all columns
    boolean useProject = !(selectItems.get(0).getExpression() instanceof AllColumns);
    if (useProject) {

      ArrayList<Column> projectSchema = new ArrayList<>();
      for (SelectItem<?> selectItem : selectItems) {
        projectSchema.add((Column) selectItem.getExpression());
      }

      operator = new ProjectOperator(operator, projectSchema);
    }

    // If Order by exists
    boolean useSort = orderByElements != null;
    if (useSort) {
      operator = new SortOperator(operator, orderByElements);
    }

    // If use distance
    boolean useDistinct = distinct != null;
    if (useDistinct) {
      operator = new DuplicateEliminationOperator(operator);
    }

    return operator;
  }
}

/**
 * Class to process the WHERE clause of the SQL statement and separate join conditions from filter
 * conditions. It uses the visitor pattern to traverse and classify each comparison expression.
 */
class WhereClauseProcessor extends ExpressionVisitorAdapter<Object> {
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
  public WhereClauseProcessor(FromItem fromItem, List<Join> joins) {
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
      BooleanEvaluator booleanEvaluator = new BooleanEvaluator();
      Boolean isTrue = comparison.accept(booleanEvaluator, null);
      if (!isTrue) {
        this.alwaysFalseCondition = true;
      }
    }
  }

  private void handleJoinCondition(
      Expression comparison, String leftTableKey, String rightTableKey) {
    if (tableOrder.get(leftTableKey) < tableOrder.get(rightTableKey)) {
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
