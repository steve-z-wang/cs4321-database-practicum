package operator;

import common.Tuple;
import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;

public class ExpressionContext {

  private final HashMap<String, Integer> columnIndexMap;
  private Tuple tuple;

  public ExpressionContext(ArrayList<Column> schema) {

    columnIndexMap = new HashMap<>();

    for (int i = 0; i < schema.size(); i++) {
      Column column = schema.get(i);
      columnIndexMap.put(column.getColumnName(), i);
    }
  }

  public void setTuple(Tuple tuple) {
    this.tuple = tuple;
  }

  public Integer getValue(String columnName) {
    if (tuple == null) {
      return null;
    }

    Integer index = columnIndexMap.get(columnName);
    if (index != null) {
      return tuple.getElementAtIndex(index);
    }
    return null;
  }
}
