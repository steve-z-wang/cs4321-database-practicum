package logicaloperator;

import net.sf.jsqlparser.schema.Table;

import java.util.List;

public class LogicalScan extends LogicalOperator {
  private final Table table;

  public LogicalScan(Table table) {
    this.table = table;
  }

  public Table getTable() {
    return table;
  }

  public LogicalOperator getChildOperator() {
    return null;
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return List.of();
  }

  @Override
  public String toString() {
    return "Leaf Scan[" + table.getName() + "]";
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }
}
