package physicaloperator.base;

import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;

/**
 * Evaluates numeric expressions.
 *
 * <p>Supports Column and LongValue
 */
public class NumericExpressionEvaluator extends ExpressionVisitorAdapter<Integer> {

  @Override
  public <S> Integer visit(LongValue longValue, S context) {
    return (int) longValue.getValue();
  }

  @Override
  public <S> Integer visit(Column column, S context) {
    ExpressionContext expressionContext = (ExpressionContext) context;
    return expressionContext.getColumnValue(column);
  }
}
