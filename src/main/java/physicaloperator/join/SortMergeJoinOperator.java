package physicaloperator.join;

import java.util.ArrayList;
import java.util.List;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.PhysicalOperator;

/**
 * Implements Sort-Merge Join algorithm. Algorithm logic: 1. Advance through both relations until
 * matching tuples are found 2. When a match is found, mark the position in right relation 3. Output
 * all matching combinations 4. When exhausted, reset right relation to mark and advance left
 * relation
 */
public class SortMergeJoinOperator extends PhysicalOperator {
  private static final Logger logger = LogManager.getLogger(SortMergeJoinOperator.class);

  private final PhysicalOperator leftOperator;
  private final PhysicalOperator rightOperator;
  private final JoinTupleComparator comparator;

  private Tuple leftTuple;
  private Tuple rightTuple;
  private int rightIndex;
  private int rightMark;
  private boolean isMarked;
  private String joinInfo;

  public SortMergeJoinOperator(
      PhysicalOperator leftOperator,
      PhysicalOperator rightOperator,
      JoinTupleComparator comparator, String joinInfo) {
    super(null);

    logger.debug("Initializing SortMergeJoinOperator");

    this.leftOperator = leftOperator;
    this.rightOperator = rightOperator;
    this.comparator = comparator;

    this.outputSchema = new ArrayList<>(leftOperator.getOutputSchema());
    this.outputSchema.addAll(rightOperator.getOutputSchema());
    this.joinInfo = joinInfo;

    // Initialize state
    this.leftTuple = leftOperator.getNextTuple();
    this.rightTuple = rightOperator.getNextTuple();
    this.rightIndex = 0;
    this.rightMark = -1;
    this.isMarked = false;
  }

  @Override
  public void reset() {
    leftOperator.reset();
    rightOperator.reset();
    leftTuple = leftOperator.getNextTuple();
    rightTuple = rightOperator.getNextTuple();
    rightIndex = 0;
    rightMark = -1;
    isMarked = false;
  }

  @Override
  public Tuple getNextTuple() {
    // Return null if either relation is exhausted
    if (leftTuple == null || rightTuple == null) {
      return null;
    }

    while (true) {
      if (!isMarked) {
        // Phase 1: Find matching tuples
        while (leftTuple != null && rightTuple != null) {
          int compareResult = comparator.compare(leftTuple, rightTuple);

          if (compareResult < 0) {
            advanceLeft();
          } else if (compareResult > 0) {
            advanceRight();
          } else {
            // Found a match
            rightMark = rightIndex;
            isMarked = true;
            break;
          }

          if (leftTuple == null || rightTuple == null) {
            return null;
          }
        }
      }

      // If we have matching tuples
      if (isMarked) {
        int compareResult = comparator.compare(leftTuple, rightTuple);

        if (compareResult == 0) {
          // Output the matching combination
          Tuple result = leftTuple.append(rightTuple);
          advanceRight();

          // If right side is exhausted, move to next left tuple
          if (rightTuple == null) {
            advanceLeft();
            resetRightToMark();
            isMarked = false;
          }

          return result;
        } else {
          // No more matches for current left tuple
          advanceLeft();
          resetRightToMark();
          isMarked = false;

          if (leftTuple == null) {
            return null;
          }
        }
      }
    }
  }

  private void advanceLeft() {
    leftTuple = leftOperator.getNextTuple();
  }

  private void advanceRight() {
    rightTuple = rightOperator.getNextTuple();
    rightIndex++;
  }

  private void resetRightToMark() {
    if (rightMark >= 0) {
      rightOperator.reset();
      rightTuple = rightOperator.getNextTuple();
      rightIndex = 0;

      // Advance to marked position
      while (rightIndex < rightMark && rightTuple != null) {
        advanceRight();
      }
    }
  }

  public String toString() {
    String thisInfo = this.joinInfo;
    List<String> leftSubTree = List.of(leftOperator.toString().split("\n"));
    List<String> rightSubTree = List.of(leftOperator.toString().split("\n"));
    StringBuilder sb = new StringBuilder();
    sb.append(thisInfo);
      for (String s : leftSubTree) {
          sb.append("\n");
          sb.append("-").append(s);
      }
      for (String s : rightSubTree) {
          sb.append("\n");
          sb.append("-").append(s);
      }
    return sb.toString();
  }
}
