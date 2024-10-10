package logicaloperator;

import net.sf.jsqlparser.schema.Table;

public class LogicalScan extends LogicalOperator {
  private final Table table;

  public LogicalScan(Table table) {
    this.table = table;
  }

  public Table getTable() {
    return table;
  }

  @Override
  public String toString() {
    return "Scan[" + table.getName() + "]";
  }
}
