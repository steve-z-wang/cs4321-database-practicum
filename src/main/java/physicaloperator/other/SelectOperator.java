package physicaloperator.other;

import java.util.ArrayList;
import model.Tuple;
import net.sf.jsqlparser.expression.Expression;
import physicaloperator.base.BooleanEvaluator;
import physicaloperator.base.ExpressionContext;
import physicaloperator.base.Operator;

/** able to perform SELECT * FROM Boats WHERE Boats.id = 4 */
public class SelectOperator extends Operator {

  private final Operator childOperator;
  private final Expression expression;
  private final ExpressionContext expressionContext;
  private final BooleanEvaluator evaluator = new BooleanEvaluator();

  public SelectOperator(Operator operator, Expression expression) {
    super(null);

    assert operator != null : "Operator cannot be null";
    assert expression != null : "Expression cannot be null";

    this.childOperator = operator;
    this.expression = expression;

    // Set output schema same as input schema
    this.outputSchema = new ArrayList<>(operator.getOutputSchema());

    // Initialize the ColumnTupleMapper with the schema
    this.expressionContext = new ExpressionContext(this.outputSchema);
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
      expressionContext.setContext(tuple.getAllElements());

      // Evaluation the expression
      isValid = expression.accept(this.evaluator, this.expressionContext);

      // Return if found
      if (isValid) {
        return tuple;
      }
    }

    return null;
  }
}
