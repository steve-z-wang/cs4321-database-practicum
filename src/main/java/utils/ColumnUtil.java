package utils;

import java.util.Comparator;
import net.sf.jsqlparser.schema.Column;

public class ColumnUtil {

  // Example static method to compare Columns
  public static int compareColumns(Column c1, Column c2) {
    String c1Name = c1.getFullyQualifiedName(true);
    String c2Name = c2.getFullyQualifiedName(true);
    return c1Name.compareTo(c2Name);
  }

  // Generic Comparator for Columns
  public static Comparator<Column> columnComparator() {
    return ColumnUtil::compareColumns;
  }
}
