package unitTest.physicaloperator.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import model.Tuple;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import physicaloperator.base.ExpressionContext;
import physicaloperator.base.NumericExpressionEvaluator;

class NumericExpressionEvaluatorTest {

  private NumericExpressionEvaluator numericExpressionEvaluator;
  private ExpressionContext expressionContext;

  @BeforeEach
  public void setUp() {
    // Initialize the NumericEvaluator
    numericExpressionEvaluator = new NumericExpressionEvaluator();

    // Create a schema with column names
    ArrayList<Column> schema = new ArrayList<>();
    schema.add(new Column("col1"));
    schema.add(new Column("col2"));
    schema.add(new Column("col3"));

    // Initialize the ExpressionContext with the schema
    expressionContext = new ExpressionContext(schema);

    // Create a Tuple with some values and set it in the ExpressionContext
    Tuple tuple = new Tuple("10,20,30"); // Assuming Tuple takes Integer array
    expressionContext.setContext(tuple.getAllElements());
  }

  @Test
  public void testVisitLongValue() {
    LongValue longValue = new LongValue(100);

    // Visit LongValue and verify the result
    Integer result = numericExpressionEvaluator.visit(longValue, expressionContext);
    assertEquals(100, result, "Expected LongValue to return its numeric value");
  }

  @Test
  public void testVisitColumn() {
    // Create a Column that matches "col1" in the schema
    Column column = new Column("col1");

    // Visit Column and verify the result
    Integer result = numericExpressionEvaluator.visit(column, expressionContext);
    assertEquals(10, result, "Expected Column 'col1' to return value 10 from the tuple");

    // Test for another column "col3"
    column = new Column("col3");
    result = numericExpressionEvaluator.visit(column, expressionContext);
    assertEquals(30, result, "Expected Column 'col3' to return value 30 from the tuple");

    // Test for a non-existing column (not in schema)
    column = new Column("col4");
    result = numericExpressionEvaluator.visit(column, expressionContext);
    assertNull(result, "Expected null for non-existing column 'col4'");
  }
}
