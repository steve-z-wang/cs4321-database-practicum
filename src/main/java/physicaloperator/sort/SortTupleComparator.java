package physicaloperator.sort;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/** Comparator for sorting tuples based on specified order by elements. */
public class SortTupleComparator implements Comparator<Tuple> {
  private final ArrayList<Integer> orderByIndices;

  /**
   * Creates a comparator that will compare tuples based on the specified order by elements.
   *
   * @param orderByElements list of columns to sort by
   * @param tupleSchema schema defining the tuple structure
   */
  public SortTupleComparator(List<OrderByElement> orderByElements, List<Column> tupleSchema) {
    this.orderByIndices = new ArrayList<>();

    for (OrderByElement orderByElement : orderByElements) {
      Column orderColumn = (Column) orderByElement.getExpression();
      String orderColumnName = orderColumn.getFullyQualifiedName(true);

      // Find matching column index in schema
      for (int i = 0; i < tupleSchema.size(); i++) {
        if (tupleSchema.get(i).getFullyQualifiedName(true).equals(orderColumnName)) {
          orderByIndices.add(i);
          break;
        }
      }
    }
  }

  @Override
  public int compare(Tuple t1, Tuple t2) {
    for (Integer tupleIndex : this.orderByIndices) {
      int value1 = t1.getElementAtIndex(tupleIndex);
      int value2 = t2.getElementAtIndex(tupleIndex);

      if (value1 != value2) {
        return Integer.compare(value1, value2);
      }
    }
    return 0;
  }
}
