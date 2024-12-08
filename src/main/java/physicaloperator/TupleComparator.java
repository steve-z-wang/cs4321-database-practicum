package physicaloperator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import util.ColumnUtil;

public abstract class TupleComparator implements Comparator<Tuple> {
  protected final List<Integer> orderByIndices;

  protected TupleComparator(List<Column> schema, List<OrderByElement> orderByElements) {
    this.orderByIndices = buildOrderedIndices(schema, orderByElements);
  }

  protected ArrayList<Integer> buildOrderedIndices(
      List<Column> schema, List<OrderByElement> orderByElements) {
    ArrayList<Integer> indexOrder = new ArrayList<>();
    for (OrderByElement orderByElement : orderByElements) {
      Column orderColumn = (Column) orderByElement.getExpression();
      boolean found = false;
      for (int i = 0; i < schema.size(); i++) {
        if (ColumnUtil.compareColumns(schema.get(i), orderColumn) == 0) {
          indexOrder.add(i);
          found = true;
          break;
        }
      }
      if (!found) {
        throw new IllegalArgumentException("Order by column not found in schema");
      }
    }
    return indexOrder;
  }

  protected int compareTupleElements(Tuple t1, Tuple t2, List<Integer> indices) {
    for (Integer index : indices) {
      int value1 = t1.getElementAtIndex(index);
      int value2 = t2.getElementAtIndex(index);
      if (value1 != value2) {
        return Integer.compare(value1, value2);
      }
    }
    return 0;
  }

  public abstract int compare(Tuple t1, Tuple t2);
}
