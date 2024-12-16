package physicaloperator;

import java.util.ArrayList;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.expression.Expression;

/** able to perform SELECT * FROM Boats WHERE Boats.id = 4 */
public class SelectOperator extends PhysicalOperator {

  private final PhysicalOperator childOperator;
  private final Expression expression;
  private final QueryConditionContext queryConditionContext;
  private final QueryConditionEvaluator evaluator = new QueryConditionEvaluator();

  public SelectOperator(PhysicalOperator operator, Expression expression) {
    super(null);

    assert operator != null : "Operator cannot be null";
    assert expression != null : "Expression cannot be null";

    this.childOperator = operator;
    this.expression = expression;

    // Set output schema same as input schema
    this.outputSchema = new ArrayList<>(operator.getOutputSchema());

    // Initialize the ColumnTupleMapper with the schema
    this.queryConditionContext = new QueryConditionContext(this.outputSchema);
  }

  @Override
  public void reset() {
    childOperator.reset();
  }

  @Override
  public Tuple getNextTuple() {

    Boolean isValid;
    Tuple tuple;

    // Loop until we find a valid tuple or run out of tuples
    while ((tuple = childOperator.getNextTuple()) != null) {

      // Update context
      queryConditionContext.setContext(tuple.getAllElements());

      // Evaluation the expression
      isValid = expression.accept(this.evaluator, this.queryConditionContext);

      // Return if found
      if (isValid) {
        return tuple;
      }
    }

    return null;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Select[Condition: ").append(expression).append("]");
    List<String> subplan = List.of(childOperator.toString().split("\n"));
    for (String s : subplan) {
      sb.append("\n");
      sb.append("-").append(s);
    }
    return sb.toString();
  }
}
