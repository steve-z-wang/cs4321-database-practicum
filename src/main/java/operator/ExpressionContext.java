package operator;

import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;

public class ExpressionContext {

  private final HashMap<String, Integer> columnIndexMap;
  private ArrayList<Integer> context;

  public ExpressionContext(ArrayList<Column> schema) {

    columnIndexMap = new HashMap<>();

    for (int i = 0; i < schema.size(); i++) {
      Column column = schema.get(i);
      columnIndexMap.put(column.getColumnName(), i);
    }
  }

  public void setContext(ArrayList<Integer> context) {
    this.context = context;
  }

  public Integer getValue(String columnName) {
    if (this.context == null) {
      return null;
    }

    Integer index = columnIndexMap.get(columnName);
    if (index != null) {
      return this.context.get(index);
    }
    return null;
  }
}
