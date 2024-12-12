package queryplan;

import config.DBCatalog;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import utils.ColumnUtil;

public class LogicalPlanBuilder {
  Logger logger = LogManager.getLogger(LogicalPlanBuilder.class);

  private List<SelectItem<?>> selectItems;
  private FromItem fromItem;
  private List<Join> joins;
  private Expression where;
  private List<OrderByElement> orderByElements;
  private Distinct distinct;

  public LogicalPlanBuilder() {}

  public LogicalOperator buildPlan(Statement stmt) {
    logger.info("Building logical plan for query: {}", stmt);

    PlainSelect plainSelect = (PlainSelect) (Select) stmt;
    extractQueryComponents(plainSelect);

    // Create the initial operator for fromItem
    Table mainTable = (Table) fromItem;
    LogicalOperator operator = new LogicalScan(mainTable);

    if (joins == null && where != null) {
      // If there are no joins but a WHERE clause exists, apply selection filtering
      operator = new LogicalSelect(operator, where);

    } else if (joins != null && where == null) {
      // If joins exist but no WHERE clause, process the joins without filtering
      operator = applySimpleJoins(operator);

    } else if (joins != null) {
      // If both joins and WHERE clause exist, process joins and apply selection filtering
      operator = applyJoins(mainTable, operator);
    }

    if (useProjection()) operator = applyProjection(operator);

    if (useOrdering()) operator = applyOrdering(operator);

    if (useDistinct()) operator = applyDistinct(operator);

    return operator;
  }

  private void extractQueryComponents(PlainSelect plainSelect) {
    selectItems = plainSelect.getSelectItems(); // projection
    fromItem = plainSelect.getFromItem();
    joins = plainSelect.getJoins();
    where = plainSelect.getWhere(); // filter conditions
    orderByElements = plainSelect.getOrderByElements(); // sort order
    distinct = plainSelect.getDistinct();
  }

  private boolean useProjection() {
    return !(selectItems.getFirst().getExpression() instanceof AllColumns);
  }

  private boolean useOrdering() {
    return orderByElements != null;
  }

  private boolean useDistinct() {
    return distinct != null;
  }

  private LogicalOperator applyOrdering(LogicalOperator operator) {
    // If Order by exists
    boolean useSort = useOrdering();
    if (useSort) {
      operator = new LogicalSort(operator, orderByElements);
    }
    return operator;
  }

  private LogicalOperator applyProjection(LogicalOperator operator) {
    // Create a project operator if not selecting all columns
    ArrayList<Column> outputSchema = new ArrayList<>();
    for (SelectItem<?> selectItem : selectItems) {
      outputSchema.add((Column) selectItem.getExpression());
    }
    operator = new LogicalProject(operator, outputSchema);
    return operator;
  }

  private LogicalOperator applySimpleJoins(LogicalOperator operator) {
    LogicalOperator previousOperator = operator;
    for (Join join : joins) {
      Table joinTable = (Table) join.getRightItem();
      operator = new LogicalScan(joinTable);
      operator = new LogicalJoin(previousOperator, operator, null);
      previousOperator = operator;
    }
    return operator;
  }

  private LogicalOperator applyJoins(Table mainTable, LogicalOperator operator) {
    // Process where clause
    QueryConditionExtractor queryConditionExtractor = new QueryConditionExtractor(fromItem, joins);
    where.accept(queryConditionExtractor, null);

    // Apply filter conditions for the main table
    Expression filterCondition = queryConditionExtractor.getFilterConditionsByTable(mainTable);
    if (filterCondition != null) {
      operator = new LogicalSelect(operator, filterCondition);
    }

    // Process joins and apply join conditions where appropriate
    LogicalOperator leftOperator = operator;
    for (Join join : joins) {
      Table joinTable = (Table) join.getRightItem();

      // Create a scan operator for each table
      operator = new LogicalScan(joinTable);

      // Apply filter conditions for each join table
      filterCondition = queryConditionExtractor.getFilterConditionsByTable(joinTable);
      if (filterCondition != null) {
        operator = new LogicalSelect(operator, filterCondition);
      }

      // Add join conditions to the join operator
      Expression joinCondition = queryConditionExtractor.getJoinConditionsByTable(joinTable);
      operator = new LogicalJoin(leftOperator, operator, joinCondition);

      leftOperator = operator;
    }
    return operator;
  }

  /**
   * Apply distinct operator to the logical plan, which consists of a sort operator followed by a
   * distinct operator.
   */
  private LogicalOperator applyDistinct(LogicalOperator operator) {

    List<OrderByElement> newOrderByElements = new ArrayList<>();
    if (useOrdering()) {

      operator = ((LogicalSort) operator).getChildOperator();
      newOrderByElements.addAll(orderByElements);
    }

    ArrayList<Column> outputSchema;
    if (useProjection()) {
      // if we are using projection, we would like to add all the columns in the select items
      outputSchema = new ArrayList<>();
      for (SelectItem<?> selectItem : selectItems) {
        outputSchema.add((Column) selectItem.getExpression());
      }
    } else {
      // if we are not using projection, we would like to add all the columns in all tables
      outputSchema = buildFullSchema();
    }

    // for each column in the output schema, we would like to add it to the order by elements
    for (Column column : outputSchema) {

      if (useOrdering()) {

        // check if the column is in the order by elements
        boolean isInOrderByElements = false;
        for (OrderByElement orderByElement : orderByElements) {
          Column orderByColumn = (Column) orderByElement.getExpression();
          if (ColumnUtil.compareColumns(column, orderByColumn) == 0) {
            isInOrderByElements = true;
            break;
          }
        }

        // if in order by elements, then we would like to skip it
        if (isInOrderByElements) {
          continue;
        }
      }

      // if not in order by elements, then we would like to add it to the project schema
      OrderByElement newOrderByElement = new OrderByElement();
      newOrderByElement.setExpression(column);
      newOrderByElements.add(newOrderByElement);
    }

    // add sort operator
    operator = new LogicalSort(operator, newOrderByElements);
    operator = new LogicalDistinct(operator);
    return operator;
  }

  private ArrayList<Column> buildFullSchema() {
    ArrayList<Column> projectSchema = new ArrayList<>();

    // get all the tables
    List<Table> tables = new ArrayList<>();
    tables.add((Table) fromItem);
    if (joins != null) {
      for (Join join : joins) {
        tables.add((Table) join.getRightItem());
      }
    }

    // get all the columns in all tables
    DBCatalog dbCatalog = DBCatalog.getInstance();
    for (Table table : tables) {
      String tableName = table.getName();

      for (Column column : dbCatalog.getSchemaForTable(tableName)) {
        // create a new column that has table with alias
        Column columnWithTableAlias = new Column(table, column.getColumnName());
        projectSchema.add(columnWithTableAlias);
      }
    }

    return projectSchema;
  }
}
