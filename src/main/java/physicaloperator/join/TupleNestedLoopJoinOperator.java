package physicaloperator.join;

import java.util.ArrayList;
import java.util.function.Supplier;
import model.Tuple;
import net.sf.jsqlparser.expression.Expression;
import physicaloperator.base.BooleanExpressionEvaluator;
import physicaloperator.base.ExpressionContext;
import physicaloperator.base.PhysicalOperator;

public class TupleNestedLoopJoinOperator extends PhysicalOperator {

  private final PhysicalOperator leftOperator;
  private final PhysicalOperator rightOperator;
  private final Expression expression;
  private final Supplier<Tuple> selectedGetNextTuple;
  private final ExpressionContext expressionContext;
  private final BooleanExpressionEvaluator evaluator;
  private Tuple leftTuple;

  public TupleNestedLoopJoinOperator(
      PhysicalOperator leftOperator, PhysicalOperator rightOperator) {
    this(leftOperator, rightOperator, null);
  }

  public TupleNestedLoopJoinOperator(
      PhysicalOperator leftOperator, PhysicalOperator rightOperator, Expression expression) {
    super(null);

    this.leftOperator = leftOperator;
    this.rightOperator = rightOperator;
    this.expression = expression;

    // Set output schema as the combine of left and right operators' schema
    this.outputSchema = new ArrayList<>(leftOperator.getOutputSchema());
    this.outputSchema.addAll(rightOperator.getOutputSchema());

    // Check if the expression is null and select the correct behavior
    if (expression == null) {
      this.selectedGetNextTuple = this::getNextTupleWithoutExpression;
      this.expressionContext = null;
      this.evaluator = null;
    } else {
      this.selectedGetNextTuple = this::getNextTupleWithExpression;
      this.expressionContext = new ExpressionContext(this.outputSchema);
      this.evaluator = new BooleanExpressionEvaluator();
    }

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

    while (this.leftTuple != null) {

      // Loop through until we find a valid combine tuple
      while ((rightTuple = rightOperator.getNextTuple()) != null) {

        Tuple combinedTuple = leftTuple.append(rightTuple);

        // Set context for expression evaluation
        this.expressionContext.setContext(combinedTuple.getAllElements());
        if (expression.accept(this.evaluator, this.expressionContext)) {
          // return when the expression is ture
          return combinedTuple;
        }
      }

      // If we run out of right tuple
      advanceToNextLeftTuple();
    }

    return null;
  }

  public Tuple getNextTupleWithoutExpression() {
    Tuple rightTuple;

    while (this.leftTuple != null) {
      rightTuple = this.rightOperator.getNextTuple();

      if (rightTuple != null) {
        return leftTuple.append(rightTuple);
      }

      advanceToNextLeftTuple();
    }

    return null;
  }

  private void advanceToNextLeftTuple() {
    leftTuple = leftOperator.getNextTuple();
    rightOperator.reset();
  }
}
