package utils;

import java.util.List;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

public class SortTupleComparator extends TupleComparator {

  public SortTupleComparator(List<Column> schema, List<OrderByElement> orderByElements) {
    super(schema, orderByElements);
  }
}
