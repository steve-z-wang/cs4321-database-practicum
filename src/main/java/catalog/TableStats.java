package catalog;

import net.sf.jsqlparser.schema.Column;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

public class TableStats {

  private final String tableName;
  private int numTuples;
  // Maps column name to ColumnStats
  private final Map<String, ColumnStats> columnStats;

  /**
   * Creates new TableStats instance for a given table
   * @param tableName Name of the table
   */
  public TableStats(String tableName) {
    this.tableName = tableName;
    this.numTuples = 0;
    this.columnStats = new HashMap<>();
  }

  /**
   * Updates statistics for a single tuple
   * @param columnName Name of the column
   * @param value Value in the tuple for this column
   */
  public void addTupleStats(String columnName, int value) {
    columnStats.computeIfAbsent(columnName, k -> new ColumnStats())
        .updateStats(value);
  }

  /**
   * Increments the total tuple count for this table
   */
  public void incrementTupleCount() {
    numTuples++;
  }

  /**
   * Writes table statistics to a writer in the stats.txt format
   */
  public void serialize(BufferedWriter writer, List<Column> schema) throws IOException {
    writer.write(getStatsString(schema));
    writer.newLine();
  }

  /**
   * Deserialize a TableStats object from a line of text from stats.txt
   * Format: tableName numTuples col1,min1,max1 col2,min2,max2 ...
   * @param line The line to parse
   * @return A new TableStats object populated with the data from the line
   * @throws IOException If the line format is invalid
   */
  public static TableStats deserialize(String line) throws IOException {
    // Split the line into parts
    String[] parts = line.trim().split("\\s+");
    if (parts.length < 2) {
      throw new IOException("Invalid stats line format: " + line);
    }

    // First two parts are table name and tuple count
    String tableName = parts[0];
    TableStats stats = new TableStats(tableName);
    try {
      stats.numTuples = Integer.parseInt(parts[1]);
    } catch (NumberFormatException e) {
      throw new IOException("Invalid tuple count: " + parts[1]);
    }

    // Process each column's statistics
    for (int i = 2; i < parts.length; i++) {
      String[] columnParts = parts[i].split(",");
      if (columnParts.length != 3) {
        throw new IOException("Invalid column stats format: " + parts[i]);
      }

      String columnName = columnParts[0];
      try {
        // Parse min and max values
        int minValue = Integer.parseInt(columnParts[1]);
        int maxValue = Integer.parseInt(columnParts[2]);

        // Create and initialize ColumnStats
        ColumnStats columnStats = new ColumnStats();
        // We need to update both min and max separately to ensure proper initialization
        columnStats.updateStats(minValue);
        columnStats.updateStats(maxValue);

        // Add to the map
        stats.columnStats.put(columnName, columnStats);
      } catch (NumberFormatException e) {
        throw new IOException("Invalid numeric value in column stats: " + parts[i]);
      }
    }

    return stats;
  }

  /**
   * Writes statistics to the stats.txt file in the required format
   * @return Formatted string representation of table statistics
   */
  public String getStatsString(List<Column> schema) {
    StringBuilder sb = new StringBuilder();

    sb.append(tableName).append(" ").append(numTuples);

    for (Column column : schema) {
      String columnName = column.getColumnName();
      ColumnStats stats = columnStats.get(columnName);
      sb.append(" ").append(columnName).append(",")
          .append(stats.getMinValue()).append(",")
          .append(stats.getMaxValue());
    }

    return sb.toString();
  }


}
