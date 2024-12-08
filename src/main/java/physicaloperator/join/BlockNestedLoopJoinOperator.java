package physicaloperator.join;

import static config.PhysicalPlanConfig.INT_SIZE;
import static config.PhysicalPlanConfig.PAGE_SIZE;

import config.PhysicalPlanConfig;
import java.util.ArrayList;
import java.util.function.Supplier;
import model.Tuple;
import net.sf.jsqlparser.expression.Expression;
import physicaloperator.PhysicalOperator;
import physicaloperator.QueryConditionContext;
import physicaloperator.QueryConditionEvaluator;

public class BlockNestedLoopJoinOperator extends PhysicalOperator {
  private final PhysicalOperator outerOperator;
  private final PhysicalOperator innerOperator;
  private final Expression condition;
  private final Supplier<Tuple> selectedGetNextTuple;
  private final QueryConditionContext queryConditionContext;
  private final QueryConditionEvaluator evaluator;

  private Tuple[] outerBlockBuffer;
  private int tuplesPerBlock;
  private int currentBufferIndex;
  private int currentBufferSize;
  private Tuple currentInnerTuple;

  public BlockNestedLoopJoinOperator(
      PhysicalOperator outerOperator, PhysicalOperator innerOperator) {
    this(outerOperator, innerOperator, null);
  }

  public BlockNestedLoopJoinOperator(
      PhysicalOperator outerOperator, PhysicalOperator innerOperator, Expression condition) {
    this(
        outerOperator,
        innerOperator,
        condition,
        PhysicalPlanConfig.getInstance().getJoinBufferPages());
  }

  public BlockNestedLoopJoinOperator(
      PhysicalOperator outerOperator,
      PhysicalOperator innerOperator,
      Expression condition,
      int JoinBufferPages) {

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
      this.queryConditionContext = null;
      this.evaluator = null;
    } else {
      this.selectedGetNextTuple = this::getNextTupleWithCondition;
      this.queryConditionContext = new QueryConditionContext(this.outputSchema);
      this.evaluator = new QueryConditionEvaluator();
    }

    // Initialize buffer
    tuplesPerBlock = JoinBufferPages * PAGE_SIZE / (this.outputSchema.size() * INT_SIZE);
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

          this.queryConditionContext.setContext(combinedTuple.getAllElements());
          if (condition.accept(this.evaluator, this.queryConditionContext)) {
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
