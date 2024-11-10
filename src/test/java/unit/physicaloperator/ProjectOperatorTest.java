package unit.physicaloperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import physicaloperator.base.Operator;
import physicaloperator.other.ProjectOperator;

class ProjectOperatorTest {

  private Operator inputOperator;
  private ProjectOperator projectOperator;

  @BeforeEach
  void setUp() {
    // Mock the input operator (even though it's abstract)
    inputOperator = Mockito.mock(Operator.class);

    // Set up the input schema (InputOperator's output schema)
    Table table = new Table("test_table");
    Column col1 = new Column(table, "col1");
    Column col2 = new Column(table, "col2");
    Column col3 = new Column(table, "col3");

    // Return the input schema when getOutputSchema is called on inputOperator
    when(inputOperator.getOutputSchema())
        .thenReturn(new ArrayList<>(Arrays.asList(col1, col2, col3)));

    // Create the output schema (columns we want to project)
    ArrayList<Column> outputSchema = new ArrayList<>(Arrays.asList(col3, col1));

    // Initialize the ProjectOperator
    projectOperator = new ProjectOperator(inputOperator, outputSchema);
  }

  @Test
  void testGetNextTuple() {
    // Prepare input tuple
    Tuple inputTuple =
        new Tuple(new ArrayList<>(Arrays.asList(1, 2, 3))); // corresponds to col1=1, col2=2, col3=3

    // Mock the input operator to return the input tuple
    when(inputOperator.getNextTuple()).thenReturn(inputTuple).thenReturn(null);

    // Get the next tuple from the ProjectOperator
    Tuple result = projectOperator.getNextTuple();

    // Verify the projection (should return tuple with col3 and col1 values: [3, 1])
    assertNotNull(result);
    List<Integer> expectedValues = Arrays.asList(3, 1);
    assertEquals(expectedValues, result.getAllElements());

    // Verify that the next tuple is null after the input tuple is consumed
    assertNull(projectOperator.getNextTuple());
  }

  @Test
  void testReset() {
    // Call reset and verify input operator's reset method is called
    projectOperator.reset();
    verify(inputOperator, times(1)).reset();
  }

  @Test
  void testNoTuples() {
    // Mock input operator to return null (no more tuples)
    when(inputOperator.getNextTuple()).thenReturn(null);

    // Ensure ProjectOperator also returns null when no tuples are available
    assertNull(projectOperator.getNextTuple());
  }
}
