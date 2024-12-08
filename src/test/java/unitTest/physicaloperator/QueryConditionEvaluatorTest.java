package unitTest.physicaloperator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import model.Tuple;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import physicaloperator.QueryConditionContext;
import physicaloperator.QueryConditionEvaluator;

class QueryConditionEvaluatorTest {

  private QueryConditionEvaluator queryConditionEvaluator;
  private QueryConditionContext queryConditionContext;
  private Tuple tuple;

  @BeforeEach
  public void setUp() {
    queryConditionEvaluator = new QueryConditionEvaluator();

    // Define a schema with three columns
    ArrayList<Column> schema = new ArrayList<>();
    schema.add(new Column(null, "col1"));
    schema.add(new Column(null, "col2"));
    schema.add(new Column(null, "col3"));

    // Initialize the expression context with the schema
    queryConditionContext = new QueryConditionContext(schema);

    // Define a tuple with values corresponding to the schema
    tuple = new Tuple("10,20,30");

    // Set the tuple in the expression context
    queryConditionContext.setContext(tuple.getAllElements());
  }

  @Test
  public void testEqualsTo() {
    EqualsTo equalsTo = new EqualsTo();
    equalsTo.setLeftExpression(new LongValue(10));
    equalsTo.setRightExpression(new Column(null, "col1"));

    Boolean result = queryConditionEvaluator.visit(equalsTo, queryConditionContext);
    assertTrue(result, "Expected column col1 to be equal to 10");

    equalsTo.setRightExpression(new LongValue(5));
    result = queryConditionEvaluator.visit(equalsTo, queryConditionContext);
    assertFalse(result, "Expected column col1 to be unequal to 5");
  }

  @Test
  public void testNotEqualsTo() {
    NotEqualsTo notEqualsTo = new NotEqualsTo();
    notEqualsTo.setLeftExpression(new Column(null, "col1"));
    notEqualsTo.setRightExpression(new LongValue(5));

    Boolean result = queryConditionEvaluator.visit(notEqualsTo, queryConditionContext);
    assertTrue(result, "Expected column col1 to be unequal to 5");

    notEqualsTo.setRightExpression(new LongValue(10));
    result = queryConditionEvaluator.visit(notEqualsTo, queryConditionContext);
    assertFalse(result, "Expected column col1 to be equal to 10");
  }

  @Test
  public void testGreaterThan() {
    GreaterThan greaterThan = new GreaterThan();
    greaterThan.setLeftExpression(new Column(null, "col2"));
    greaterThan.setRightExpression(new LongValue(15));

    Boolean result = queryConditionEvaluator.visit(greaterThan, queryConditionContext);
    assertTrue(result, "Expected column col2 to be greater than 15");

    greaterThan.setRightExpression(new LongValue(25));
    result = queryConditionEvaluator.visit(greaterThan, queryConditionContext);
    assertFalse(result, "Expected column col2 to be less than 25");
  }

  @Test
  public void testGreaterThanEquals() {
    GreaterThanEquals greaterThanEquals = new GreaterThanEquals();
    greaterThanEquals.setLeftExpression(new Column(null, "col2"));
    greaterThanEquals.setRightExpression(new LongValue(20));

    Boolean result = queryConditionEvaluator.visit(greaterThanEquals, queryConditionContext);
    assertTrue(result, "Expected column col2 to be greater than or equal to 20");

    greaterThanEquals.setRightExpression(new LongValue(25));
    result = queryConditionEvaluator.visit(greaterThanEquals, queryConditionContext);
    assertFalse(result, "Expected column col2 to be less than 25");
  }

  @Test
  public void testMinorThan() {
    MinorThan minorThan = new MinorThan();
    minorThan.setLeftExpression(new Column(null, "col3"));
    minorThan.setRightExpression(new LongValue(35));

    Boolean result = queryConditionEvaluator.visit(minorThan, queryConditionContext);
    assertTrue(result, "Expected column col3 to be less than 35");

    minorThan.setRightExpression(new LongValue(25));
    result = queryConditionEvaluator.visit(minorThan, queryConditionContext);
    assertFalse(result, "Expected column col3 to be greater than 25");
  }

  @Test
  public void testMinorThanEquals() {
    MinorThanEquals minorThanEquals = new MinorThanEquals();
    minorThanEquals.setLeftExpression(new Column(null, "col3"));
    minorThanEquals.setRightExpression(new LongValue(30));

    Boolean result = queryConditionEvaluator.visit(minorThanEquals, queryConditionContext);
    assertTrue(result, "Expected column col3 to be less than or equal to 30");

    minorThanEquals.setRightExpression(new LongValue(25));
    result = queryConditionEvaluator.visit(minorThanEquals, queryConditionContext);
    assertFalse(result, "Expected column col3 to be greater than 25");
  }

  @Test
  public void testAndExpression() {
    EqualsTo leftExpr = new EqualsTo();
    leftExpr.setLeftExpression(new Column(null, "col1"));
    leftExpr.setRightExpression(new LongValue(10));

    EqualsTo rightExpr = new EqualsTo();
    rightExpr.setLeftExpression(new Column(null, "col2"));
    rightExpr.setRightExpression(new LongValue(20));

    AndExpression andExpression = new AndExpression(leftExpr, rightExpr);
    Boolean result = queryConditionEvaluator.visit(andExpression, queryConditionContext);
    assertTrue(result, "Expected both conditions to be true");

    // Now test with one false condition
    rightExpr.setRightExpression(new LongValue(30));
    result = queryConditionEvaluator.visit(andExpression, queryConditionContext);
    assertFalse(result, "Expected one condition to be false");
  }
}
