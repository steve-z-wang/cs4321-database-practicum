package unit.physicaloperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import physicaloperator.base.ExpressionContext;

class ExpressionContextTest {

  private ExpressionContext expressionContext;

  @BeforeEach
  public void setUp() {
    // Initialize the schema with columns (assuming these are table columns)
    ArrayList<Column> schema = new ArrayList<>();
    schema.add(new Column(null, "col1"));
    schema.add(new Column(null, "col2"));
    schema.add(new Column(null, "col3"));

    // Initialize ExpressionContext with the schema
    expressionContext = new ExpressionContext(schema);
  }

  @Test
  public void testSetAndGetValue() {
    // Create a Tuple with values and set it in the ExpressionContext
    Tuple tuple = new Tuple("10,20,30"); // Assuming Tuple takes a comma-separated string
    expressionContext.setContext(tuple.getAllElements());

    // Retrieve values by Column object
    Integer value1 = expressionContext.getValue(new Column(null, "col1"));
    assertEquals(10, value1, "Expected value for 'col1' is 10");

    Integer value2 = expressionContext.getValue(new Column(null, "col2"));
    assertEquals(20, value2, "Expected value for 'col2' is 20");

    Integer value3 = expressionContext.getValue(new Column(null, "col3"));
    assertEquals(30, value3, "Expected value for 'col3' is 30");
  }

  @Test
  public void testGetValueNonExistentColumn() {
    // Create a Tuple and set it in the ExpressionContext
    Tuple tuple = new Tuple("10,20,30");
    expressionContext.setContext(tuple.getAllElements());

    // Try to get a value for a non-existent column
    Integer value = expressionContext.getValue(new Column(null, "col4"));
    assertNull(value, "Expected null for non-existent column 'col4'");
  }

  @Test
  public void testGetValueBeforeSettingTuple() {
    // Attempt to get a value from the context before setting a tuple
    Integer value = expressionContext.getValue(new Column(null, "col1"));
    assertNull(value, "Expected null when no tuple is set");
  }

  @Test
  public void testSetNewTuple() {
    // Set the first tuple
    Tuple tuple1 = new Tuple("10,20,30");
    expressionContext.setContext(tuple1.getAllElements());
    assertEquals(10, expressionContext.getValue(new Column(null, "col1")));

    // Set a new tuple and verify the updated values
    Tuple tuple2 = new Tuple("40,50,60");
    expressionContext.setContext(tuple2.getAllElements());
    assertEquals(
        40,
        expressionContext.getValue(new Column(null, "col1")),
        "Expected value to update to 40 after setting new tuple");
    assertEquals(
        50,
        expressionContext.getValue(new Column(null, "col2")),
        "Expected value to update to 50 after setting new tuple");
    assertEquals(
        60,
        expressionContext.getValue(new Column(null, "col3")),
        "Expected value to update to 60 after setting new tuple");
  }
}
