package builder;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.base.PhysicalOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * The expression would only be an AndExpression or an EqualsTo expression,
 *
 * We know the left and right operators and their columns
 * we also have a list of condition / expression that must has both column from left and right operator
 *
 * so for each expression's left and right column, we first check if its from left or right operator
 * then we add the expression to the list of condition for that operator
 *
 */

public class SMJConditionExtractor extends ExpressionVisitorAdapter<Void> {

    Logger logger = LogManager.getLogger(SMJConditionExtractor.class);

    private final List<Column> leftColumns;
    private final List<Column> rightColumns;

    private final List<OrderByElement> leftChildSortOrder = new ArrayList<>();
    private final List<OrderByElement> rightChildSortOrder = new ArrayList<>();

    private boolean isValidSortMergeJoin = true;

    SMJConditionExtractor(PhysicalOperator leftOperator, PhysicalOperator rightOperator) {
        leftColumns = leftOperator.getOutputSchema();
        rightColumns = rightOperator.getOutputSchema();
    }

    private String getColumnName(Column column) {
        // get the name with alias if exist
        return column.getFullyQualifiedName(true);
    }

    private boolean isFromLeftOperator(Column column) {
        String columnName = getColumnName(column);
        for (Column leftColumn : leftColumns) {
            if (getColumnName(leftColumn).equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    private boolean isFromRightOperator(Column column) {
        String columnName = getColumnName(column);
        for (Column rightColumn : rightColumns) {
            if (getColumnName(rightColumn).equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public <S> Void visit(AndExpression expression, S context) {
        Expression left = expression.getLeftExpression();
        left.accept(this, context);

        Expression right = expression.getRightExpression();
        right.accept(this, context);

        return null;
    }

    @Override
    public <S> Void visit(EqualsTo expression, S context) {
        Column leftColumn = (Column) expression.getLeftExpression();
        Column rightColumn = (Column) expression.getRightExpression();

        if (isFromLeftOperator(rightColumn) && isFromRightOperator(leftColumn)) {
            // swap the columns
            Column temp = leftColumn;
            leftColumn = rightColumn;
            rightColumn = temp;
        }

        if (isFromLeftOperator(leftColumn) && isFromRightOperator(rightColumn)) {
            // add the expression to the list of condition for the left operator
            OrderByElement leftOrderByElement = new OrderByElement();
            leftOrderByElement.setExpression(leftColumn);
            leftChildSortOrder.add(leftOrderByElement);

            OrderByElement rightOrderByElement = new OrderByElement();
            rightOrderByElement.setExpression(rightColumn);
            rightChildSortOrder.add(rightOrderByElement);

        } else {
            logger.error("Invalid join condition: column not found in either operator");
        }

        return null;
    }

    @Override
    public <S> Void visit(NotEqualsTo expression, S context) {
        isValidSortMergeJoin = false;
        return null;
    }

    @Override
    public <S> Void visit(GreaterThan expression, S context) {
        isValidSortMergeJoin = false;
        return null;
    }

    @Override
    public <S> Void visit(GreaterThanEquals expression, S context) {
        isValidSortMergeJoin = false;
        return null;
    }

    @Override
    public <S> Void visit(MinorThan minorThan, S context) {
        isValidSortMergeJoin = false;
        return null;
    }

    @Override
    public <S> Void visit(MinorThanEquals minorThanEquals, S context) {
        isValidSortMergeJoin = false;
        return null;
    }

    public boolean isValidSortMergeJoin() {
        return isValidSortMergeJoin;
    }

    public List<OrderByElement> getLeftChildSortOrder() {
        return leftChildSortOrder;
    }

    public List<OrderByElement> getRightChildSortOrder() {
        return rightChildSortOrder;
    }
}
