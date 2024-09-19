package operator;

import common.Tuple;
import net.sf.jsqlparser.expression.Expression;

/** able to perform SELECT * FROM Boats WHERE Boats.id = 4 */
public class SelectOperator extends Operator {

  private final Operator inputOperator;
  private final Expression expression;
  private final ExpressionContext context;
  private final BooleanEvaluator evaluator = new BooleanEvaluator();

  public SelectOperator(Operator operator, Expression expression) {
    super(operator.getOutputSchema());

    this.inputOperator = operator;
    this.expression = expression;

    // Initialize the ColumnTupleMapper with the schema
    this.context = new ExpressionContext(operator.getOutputSchema());
  }

  @Override
  public void reset() {
    inputOperator.reset();
  }

  @Override
  public Tuple getNextTuple() {

    // Loop until we find a valid tuple or run out of tuples
    Tuple tuple = null;
    while ((tuple = inputOperator.getNextTuple()) != null) {

      context.setTuple(tuple);
      Boolean isValid = expression.accept(evaluator, context);
      if (isValid) {
        break;
      }
    }

    return tuple;
  }
}
