package physicaloperator.join;

import java.util.ArrayList;
import java.util.function.Supplier;
import model.Tuple;
import net.sf.jsqlparser.expression.Expression;
import physicaloperator.base.BooleanEvaluator;
import physicaloperator.base.ExpressionContext;
import physicaloperator.base.Operator;

public class BlockNestedLoopJoinOperator extends Operator {
  private static final int PAGE_SIZE = 4096;

  private final Operator outerOperator;
  private final Operator innerOperator;
  private final Expression condition;
  private final Supplier<Tuple> selectedGetNextTuple;
  private final ExpressionContext expressionContext;
  private final BooleanEvaluator evaluator;

  private Tuple[] outerBlockBuffer;
  private int tuplesPerBlock;
  private int currentBufferIndex;
  private int currentBufferSize;
  private Tuple currentInnerTuple;

  public BlockNestedLoopJoinOperator(
      Operator outerOperator, Operator innerOperator, Expression condition, int pagesPerBlock) {
    super(null);

    this.outerOperator = outerOperator;
    this.innerOperator = innerOperator;
    this.condition = condition;

    // Set output schema as the combine of left and right operators' schema
    this.outputSchema = new ArrayList<>(outerOperator.getOutputSchema());
    this.outputSchema.addAll(innerOperator.getOutputSchema());

    // Check if the condition is null and select the correct behavior
    if (condition == null) {
      this.selectedGetNextTuple = this::getNextTupleWithoutCondition;
      this.expressionContext = null;
      this.evaluator = null;
    } else {
      this.selectedGetNextTuple = this::getNextTupleWithCondition;
      this.expressionContext = new ExpressionContext(this.outputSchema);
      this.evaluator = new BooleanEvaluator();
    }

    // Initialize buffer
    tuplesPerBlock = PAGE_SIZE / this.outputSchema.size() * pagesPerBlock;
    outerBlockBuffer = new Tuple[tuplesPerBlock];

    loadNextBlock();
  }

  private boolean loadNextBlock() {
    currentBufferSize = 0;
    currentBufferIndex = 0;

    Tuple tuple;
    while (currentBufferSize < outerBlockBuffer.length
        && (tuple = outerOperator.getNextTuple()) != null) {
      outerBlockBuffer[currentBufferSize++] = tuple;
    }

    innerOperator.reset();
    currentInnerTuple = innerOperator.getNextTuple();

    return currentBufferSize > 0;
  }

  @Override
  public void reset() {
    outerOperator.reset();
    innerOperator.reset();

    // Clear the buffer
    for (int i = 0; i < currentBufferSize; i++) {
      outerBlockBuffer[i] = null;
    }

    // Load the first block
    loadNextBlock();
  }

  @Override
  public Tuple getNextTuple() {
    return selectedGetNextTuple.get();
  }

  private Tuple getNextTupleWithCondition() {
    // loop through each block
    while (currentBufferSize > 0) {
      // loop through inner tuples
      while (currentInnerTuple != null) {
        // loop through the block
        while (currentBufferIndex < currentBufferSize) {
          Tuple outerTuple = outerBlockBuffer[currentBufferIndex++];
          Tuple combinedTuple = outerTuple.append(currentInnerTuple);

          this.expressionContext.setContext(combinedTuple.getAllElements());
          if (condition.accept(this.evaluator, this.expressionContext)) {
            return combinedTuple;
          }
        }
        currentBufferIndex = 0;
        currentInnerTuple = innerOperator.getNextTuple();
      }
      if (!loadNextBlock()) {
        return null;
      }
    }
    return null;
  }

  private Tuple getNextTupleWithoutCondition() {
    // loop through each block
    while (currentBufferSize > 0) {
      // loop through inner tuples
      while (currentInnerTuple != null) {
        // loop through the block
        if (currentBufferIndex < currentBufferSize) {
          Tuple outerTuple = outerBlockBuffer[currentBufferIndex++];
          return outerTuple.append(currentInnerTuple);
        }
        currentBufferIndex = 0;
        currentInnerTuple = innerOperator.getNextTuple();
      }
      if (!loadNextBlock()) {
        return null;
      }
    }
    return null;
  }
}
