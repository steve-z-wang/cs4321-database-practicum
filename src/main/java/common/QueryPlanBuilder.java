package common;

import java.util.ArrayList;
import java.util.List;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.expression.Expression;
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
import physicaloperator.DuplicateEliminationOperator;
import physicaloperator.JoinOperator;
import physicaloperator.Operator;
import physicaloperator.ProjectOperator;
import physicaloperator.ScanOperator;
import physicaloperator.SelectOperator;
import physicaloperator.SortOperator;

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
