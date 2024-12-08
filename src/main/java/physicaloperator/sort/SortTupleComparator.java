package physicaloperator.sort;

import java.util.List;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import physicaloperator.TupleComparator;

public class SortTupleComparator extends TupleComparator {

  public SortTupleComparator(List<Column> schema, List<OrderByElement> orderByElements) {
    super(schema, orderByElements);
  }

  @Override
  public int compare(Tuple t1, Tuple t2) {
    return compareTupleElements(t1, t2, orderByIndices);
  }
}
