package physicaloperator;

import common.Tuple;
import java.util.ArrayList;
import java.util.function.Supplier;
import net.sf.jsqlparser.expression.Expression;

public class JoinOperator extends Operator {

  private final Operator leftOperator;
  private final Operator rightOperator;
  private final Expression expression;
  private final Supplier<Tuple> selectedGetNextTuple;
  private final ExpressionContext expressionContext;
  private final BooleanEvaluator evaluator = new BooleanEvaluator();
  private Tuple leftTuple;

  public JoinOperator(Operator leftOperator, Operator rightOperator) {
    super(null);

    this.leftOperator = leftOperator;
    this.rightOperator = rightOperator;
    this.expression = null;

    // Set output schema as the combine of left and right operators' schema
    this.outputSchema = new ArrayList<>(leftOperator.getOutputSchema());
    this.outputSchema.addAll(rightOperator.getOutputSchema());

    // Context
    this.selectedGetNextTuple = this::getNextTupleWithoutExpression;
    this.expressionContext = null;

    // Set first left tuple
    this.leftTuple = leftOperator.getNextTuple();
  }

  public JoinOperator(Operator leftOperator, Operator rightOperator, Expression expression) {
    super(null);

    this.leftOperator = leftOperator;
    this.rightOperator = rightOperator;
    this.expression = expression;

    // Set output schema as the combine of left and right operators' schema
    this.outputSchema = new ArrayList<>(leftOperator.getOutputSchema());
    this.outputSchema.addAll(rightOperator.getOutputSchema());

    this.selectedGetNextTuple = this::getNextTupleWithExpression;
    this.expressionContext = new ExpressionContext(this.outputSchema);

    // Set first left tuple
    this.leftTuple = leftOperator.getNextTuple();
  }

  @Override
  public void reset() {
    this.leftOperator.reset();
    this.rightOperator.reset();

    this.leftTuple = leftOperator.getNextTuple();
  }

  @Override
  public Tuple getNextTuple() {
    return selectedGetNextTuple.get();
  }

  public Tuple getNextTupleWithExpression() {
    Tuple rightTuple;
    ArrayList<Integer> columnValues;
    Boolean isValid;

    while (this.leftTuple != null) {

      // Loop through until we find a valid combine tuple
      while ((rightTuple = rightOperator.getNextTuple()) != null) {

        // Concat values from left and right
        columnValues = new ArrayList<>(leftTuple.getAllElements());
        columnValues.addAll(rightTuple.getAllElements());

        // Set context for expression evaluation
        this.expressionContext.setContext(columnValues);

        // Evaluate the expression
        isValid = expression.accept(this.evaluator, this.expressionContext);

        // return if found
        if (isValid) {
          return new Tuple(columnValues);
        }
      }

      // if we run out of right tuple and did not find a valid tuple
      // Get a new left tuple
      this.leftTuple = leftOperator.getNextTuple();

      // Reset the right
      this.rightOperator.reset();
    }

    return null;
  }

  public Tuple getNextTupleWithoutExpression() {

    Tuple rightTuple;
    ArrayList<Integer> columnValues;

    while (this.leftTuple != null) {

      rightTuple = this.rightOperator.getNextTuple();

      if (rightTuple != null) {

        columnValues = new ArrayList<>(leftTuple.getAllElements());
        columnValues.addAll(rightTuple.getAllElements());

        return new Tuple(columnValues);
      }

      this.leftTuple = leftOperator.getNextTuple();
      this.rightOperator.reset();
    }

    return null;
  }
}
