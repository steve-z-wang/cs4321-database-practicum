package unitTest.physicaloperator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import physicaloperator.PhysicalOperator;
import physicaloperator.SelectOperator;

/** Test cases for the SelectOperator class. */
class SelectOperatorTest {

  private PhysicalOperator inputOperator;
  private Expression expression;
  private SelectOperator selectOperator;
  private List<Column> schema;

  @BeforeEach
  void setUp() {
    // Mock the input operator
    inputOperator = mock(PhysicalOperator.class);

    // Define the schema
    Table table = new Table("Employees");
    Column idColumn = new Column(table, "id");
    Column ageColumn = new Column(table, "age");
    Column salaryColumn = new Column(table, "salary");

    schema = Arrays.asList(idColumn, ageColumn, salaryColumn);

    // Mock the output schema of the input operator
    when(inputOperator.getOutputSchema()).thenReturn(new ArrayList<>(schema));
  }

  /** Tests that the SelectOperator correctly filters tuples where Employees.salary > 60000. */
  @Test
  void testSelectOperatorWithValidExpression() throws Exception {
    // Create tuples
    Tuple tuple1 = new Tuple(new ArrayList<>(Arrays.asList(1, 25, 50000)));
    Tuple tuple2 = new Tuple(new ArrayList<>(Arrays.asList(2, 30, 60000)));
    Tuple tuple3 = new Tuple(new ArrayList<>(Arrays.asList(3, 35, 70000)));

    // Mock the input operator to return the tuples
    when(inputOperator.getNextTuple())
        .thenReturn(tuple1)
        .thenReturn(tuple2)
        .thenReturn(tuple3)
        .thenReturn(null);

    // Parse the expression
    expression = CCJSqlParserUtil.parseCondExpression("Employees.salary > 60000");

    // Initialize the SelectOperator
    selectOperator = new SelectOperator(inputOperator, expression);

    // Get the next tuple from SelectOperator
    Tuple resultTuple = selectOperator.getNextTuple();

    // Verify the tuple matches the expected condition
    assertNotNull(resultTuple, "Expected a tuple but got null");
    assertEquals(3, resultTuple.getElementAtIndex(0), "Expected id to be 3");
    assertEquals(35, resultTuple.getElementAtIndex(1), "Expected age to be 35");
    assertEquals(70000, resultTuple.getElementAtIndex(2), "Expected salary to be 70000");

    // Next call should return null since only one tuple matches
    resultTuple = selectOperator.getNextTuple();
    assertNull(resultTuple, "Expected no more tuples");
  }

  /**
   * Tests that the SelectOperator returns no tuples when the expression doesn't match any tuple.
   */
  @Test
  void testSelectOperatorWithNoMatchingTuples() throws Exception {
    // Create tuples
    Tuple tuple1 = new Tuple(new ArrayList<>(Arrays.asList(1, 25, 50000)));
    Tuple tuple2 = new Tuple(new ArrayList<>(Arrays.asList(2, 30, 60000)));
    Tuple tuple3 = new Tuple(new ArrayList<>(Arrays.asList(3, 35, 70000)));

    // Mock the input operator to return the tuples
    when(inputOperator.getNextTuple())
        .thenReturn(tuple1)
        .thenReturn(tuple2)
        .thenReturn(tuple3)
        .thenReturn(null);

    // Parse the expression that doesn't match any tuple
    expression = CCJSqlParserUtil.parseCondExpression("Employees.salary > 100000");

    // Initialize the SelectOperator
    selectOperator = new SelectOperator(inputOperator, expression);

    // Get the next tuple from SelectOperator
    Tuple resultTuple = selectOperator.getNextTuple();

    // Verify that no tuples match the condition
    assertNull(resultTuple, "Expected no tuples to match the expression");
  }

  /** Tests the reset functionality of the SelectOperator. */
  @Test
  void testSelectOperatorReset() throws Exception {
    // Create a tuple that matches the condition
    Tuple tuple = new Tuple(new ArrayList<>(Arrays.asList(3, 35, 70000)));

    // Mock the input operator to return the tuple and then null
    when(inputOperator.getNextTuple()).thenReturn(tuple).thenReturn(null);

    // Parse the expression
    expression = CCJSqlParserUtil.parseCondExpression("Employees.salary > 60000");

    // Initialize the SelectOperator
    selectOperator = new SelectOperator(inputOperator, expression);

    // Get the first tuple
    Tuple resultTuple = selectOperator.getNextTuple();
    assertNotNull(resultTuple, "Expected a tuple but got null");
    assertEquals(3, resultTuple.getElementAtIndex(0), "Expected id to be 3");
    assertEquals(35, resultTuple.getElementAtIndex(1), "Expected age to be 35");
    assertEquals(70000, resultTuple.getElementAtIndex(2), "Expected salary to be 70000");

    // No more tuples
    resultTuple = selectOperator.getNextTuple();
    assertNull(resultTuple, "Expected no more tuples");

    // Reset the operator
    selectOperator.reset();

    // Reset the inputOperator mock
    reset(inputOperator);

    // Mock the input operator again after reset
    when(inputOperator.getNextTuple()).thenReturn(tuple).thenReturn(null);

    // Get the tuple again after reset
    resultTuple = selectOperator.getNextTuple();
    assertNotNull(resultTuple, "Expected a tuple after reset but got null");
    assertEquals(3, resultTuple.getElementAtIndex(0), "Expected id to be 3 after reset");
    assertEquals(35, resultTuple.getElementAtIndex(1), "Expected age to be 35 after reset");
    assertEquals(
        70000, resultTuple.getElementAtIndex(2), "Expected salary to be 70000 after reset");

    // No more tuples after consuming again
    resultTuple = selectOperator.getNextTuple();
    assertNull(resultTuple, "Expected no more tuples after reset");
  }
}
