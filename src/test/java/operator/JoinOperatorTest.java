package operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import common.Tuple;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class JoinOperatorTest {

  private Operator leftOperator;
  private Operator rightOperator;
  private Expression expression;
  private JoinOperator joinOperator;

  @BeforeEach
  void setUp() {
    // Mock the left and right operators
    leftOperator = Mockito.mock(Operator.class);
    rightOperator = Mockito.mock(Operator.class);
    expression = Mockito.mock(Expression.class);

    // Set up the output schemas for left and right operators
    Table leftTable = new Table("left_table");
    Column leftCol1 = new Column(leftTable, "left_col1");
    Column leftCol2 = new Column(leftTable, "left_col2");

    Table rightTable = new Table("right_table");
    Column rightCol1 = new Column(rightTable, "right_col1");
    Column rightCol2 = new Column(rightTable, "right_col2");

    // Mock schema for both operators
    when(leftOperator.getOutputSchema())
        .thenReturn(new ArrayList<>(Arrays.asList(leftCol1, leftCol2)));
    when(rightOperator.getOutputSchema())
        .thenReturn(new ArrayList<>(Arrays.asList(rightCol1, rightCol2)));
  }

  @Test
  void testGetNextTupleWithoutExpression() {

    // Prepare tuples from the left and right operators
    Tuple leftTuple =
        new Tuple(new ArrayList<>(Arrays.asList(1, 2))); // corresponds to left_col1=1, left_col2=2
    Tuple rightTuple =
        new Tuple(
            new ArrayList<>(Arrays.asList(3, 4))); // corresponds to right_col1=3, right_col2=4

    // Mock the left and right operators to return tuples
    when(leftOperator.getNextTuple())
        .thenReturn(leftTuple)
        .thenReturn(null); // Left operator returns a tuple and then null
    when(rightOperator.getNextTuple())
        .thenReturn(rightTuple)
        .thenReturn(null); // Right operator returns a tuple and then null

    // Initialize the JoinOperator
    joinOperator = new JoinOperator(leftOperator, rightOperator, null);

    // Test getNextTuple
    Tuple result = joinOperator.getNextTuple();

    // Verify the concatenated tuple
    assertNotNull(result);
    List<Integer> expectedValues =
        Arrays.asList(1, 2, 3, 4); // Concatenation of left and right tuples
    assertEquals(expectedValues, result.getAllElements());

    // Verify that the next tuple is null after consuming all tuples
    assertNull(joinOperator.getNextTuple());
  }

  @Test
  void testGetNextTupleWithExpression() {

    // Prepare tuples from the left and right operators
    Tuple leftTuple =
        new Tuple(new ArrayList<>(Arrays.asList(1, 2))); // corresponds to left_col1=1, left_col2=2
    Tuple rightTuple =
        new Tuple(
            new ArrayList<>(Arrays.asList(3, 4))); // corresponds to right_col1=3, right_col2=4

    // Mock the left and right operators to return tuples
    when(leftOperator.getNextTuple()).thenReturn(leftTuple).thenReturn(null);
    when(rightOperator.getNextTuple()).thenReturn(rightTuple).thenReturn(null);

    // Initialize JoinOperator with expression
    joinOperator = new JoinOperator(leftOperator, rightOperator, expression);

    // Mock expression evaluation to return true
    when(expression.accept(any(), any())).thenReturn(true);

    // Test getNextTuple
    Tuple result = joinOperator.getNextTuple();

    // Verify the concatenated tuple with expression evaluated to true
    assertNotNull(result);
    List<Integer> expectedValues =
        Arrays.asList(1, 2, 3, 4); // Concatenation of left and right tuples
    assertEquals(expectedValues, result.getAllElements());

    // Verify that the next tuple is null after consuming all tuples
    assertNull(joinOperator.getNextTuple());
  }

  @Test
  void testReset() {
    // Call reset and verify that both left and right operators' reset methods are called
    joinOperator = new JoinOperator(leftOperator, rightOperator, null);

    joinOperator.reset();
    verify(leftOperator, times(1)).reset();
    verify(rightOperator, times(1)).reset();
  }

  @Test
  void testNoTuples() {
    // Mock both operators to return null (no tuples)
    when(leftOperator.getNextTuple()).thenReturn(null);
    when(rightOperator.getNextTuple()).thenReturn(null);

    joinOperator = new JoinOperator(leftOperator, rightOperator, null);

    // Ensure JoinOperator returns null when no tuples are available
    assertNull(joinOperator.getNextTuple());
  }
}
