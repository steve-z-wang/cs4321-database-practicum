package builder;

import java.util.ArrayList;
import java.util.List;
import logicaloperator.*;
import logicaloperator.LogicalOperator;
import logicaloperator.LogicalScan;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

public class LogicalPlanBuilder {

  public LogicalPlanBuilder() {}

  public LogicalOperator buildPlan(Statement stmt) {

    PlainSelect plainSelect = (PlainSelect) (Select) stmt;

    // Extract
    List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
    FromItem fromItem = plainSelect.getFromItem();
    List<Join> Joins = plainSelect.getJoins();
    Expression where = plainSelect.getWhere();
    List<OrderByElement> orderByElements = plainSelect.getOrderByElements();
    Distinct distinct = plainSelect.getDistinct();

    // Create the initial operator for fromItem
    Table mainTable = (Table) fromItem;
    LogicalOperator operator = new LogicalScan(mainTable);

    // If there are no joins but a WHERE clause exists, apply selection filtering
    if (Joins == null && where != null) {
      operator = new LogicalSelect(operator, where);
    }
    // If joins exist but no WHERE clause, process the joins without filtering
    else if (Joins != null && where == null) {

      LogicalOperator previousOperator = operator;
      for (Join join : Joins) {
        Table joinTable = (Table) join.getRightItem();
        operator = new LogicalScan(joinTable);
        operator = new LogicalJoin(previousOperator, operator, null);
        previousOperator = operator;
      }
    } else if (Joins != null && where != null) {

      // Process where clause
      WhereClauseProcessor whereClauseProcessor = new WhereClauseProcessor(fromItem, Joins);
      where.accept(whereClauseProcessor, null);

      // Filter conditions for the main table
      Expression filterCondition = whereClauseProcessor.getFilterConditionsByTable(mainTable);
      if (filterCondition != null) {
        operator = new LogicalSelect(operator, filterCondition);
      }

      // Process joins and apply join conditions where appropriate
      LogicalOperator leftOperator = operator;
      for (Join join : Joins) {
        Table joinTable = (Table) join.getRightItem();

        // Create a scan operator for each table
        operator = new LogicalScan(joinTable);

        // Apply filter conditions for each join table
        filterCondition = whereClauseProcessor.getFilterConditionsByTable(joinTable);
        if (filterCondition != null) {
          operator = new LogicalSelect(operator, filterCondition);
        }

        // Add join conditions to the join operator
        Expression joinCondition = whereClauseProcessor.getJoinConditionsByTable(joinTable);
        operator = new LogicalJoin(leftOperator, operator, joinCondition);

        leftOperator = operator;
      }
    }

    // Create a project operator if not selecting all columns
    boolean useProject = !(selectItems.get(0).getExpression() instanceof AllColumns);
    if (useProject) {

      ArrayList<Column> projectSchema = new ArrayList<>();
      for (SelectItem<?> selectItem : selectItems) {
        projectSchema.add((Column) selectItem.getExpression());
      }

      operator = new LogicalProject(operator, projectSchema);
    }

    // If Order by exists
    boolean useSort = orderByElements != null;
    if (useSort) {
      operator = new LogicalSort(operator, orderByElements);
    }

    // If use distance
    boolean useDistinct = distinct != null;
    if (useDistinct) {
      operator = new LogicalDistinct(operator);
    }

    return operator;
  }
}
