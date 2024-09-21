package operator;

import java.util.ArrayList;
import java.util.HashMap;
import net.sf.jsqlparser.schema.Column;

public class ExpressionContext {

  private final HashMap<String, Integer> columnNameToIndexMap;
  private ArrayList<Integer> context;

  public ExpressionContext(ArrayList<Column> contextSchema) {

    columnNameToIndexMap = new HashMap<>();

    for (int i = 0; i < contextSchema.size(); i++) {
      Column column = contextSchema.get(i);
      String key = getColumnKey(column);
      columnNameToIndexMap.put(key, i);
    }
  }

  public void setContext(ArrayList<Integer> context) {
    this.context = context;
  }

  public Integer getValue(Column column) {
    // If context is not set return null
    if (this.context == null) {
      return null;
    }

    Integer index = columnNameToIndexMap.get(getColumnKey(column));
    if (index != null) {
      return this.context.get(index);
    }
    return null;
  }

  public static String getColumnKey(Column column) {
    // return alias name if exist
    return column.getFullyQualifiedName(true);
  }
}
