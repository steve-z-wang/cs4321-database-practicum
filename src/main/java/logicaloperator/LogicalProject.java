package logicaloperator;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.schema.Column;

public class LogicalProject extends LogicalOperator {

  private final LogicalOperator childOperator;
  private final ArrayList<Column> outputSchema;

  public LogicalProject(LogicalOperator child, ArrayList<Column> outputSchema) {
    this.childOperator = child;
    this.outputSchema = outputSchema;
  }

  public LogicalOperator getChildOperator() {
    return childOperator;
  }

  public ArrayList<Column> getOutputSchema() {
    return outputSchema;
  }

  @Override
  public String toString() {
    return "Project[" + childOperator.toString() + "]";
  }

  @Override
  public void accept(LogicalOperatorVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public List<LogicalOperator> getChildren() {
    return List.of(childOperator);
  }
}
