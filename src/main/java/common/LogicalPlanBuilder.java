package common;

import java.util.List;
import logicaloperator.LogicalOperator;
import logicaloperator.LogicalScan;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

public class LogicalPlanBuilder {

  public LogicalPlanBuilder() {}

  public LogicalOperator buildLogicalPlan(Statement stmt) {

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
    LogicalOperator operator = new LogicalScan(mainTable);

    // TODO
    // Implement the rest of the LogicalPlanBuilder
    // Copy the logic from QueryPlanBuilder and modify it to build logical operators
    // after that
    // create the PhysicalPlanBuilder class and implement the buildPhysicalPlan method
    // which will take the logical operator and convert it to a physical operator using the Visitor
    // pattern

    return operator;
  }
}
