package catalog;

public class TableStats {

  private String tableName;
  private int numTuples;
  // Maps column name to ColumnStats
  private Map<String, ColumnStats> columnStats;

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
   * Inner class to hold statistics for a single column
   */
  private static class ColumnStats {
    private int minValue;
    private int maxValue;
    private int distinctValues; // V-value as described in the project spec

    public ColumnStats() {
      this.minValue = Integer.MAX_VALUE;
      this.maxValue = Integer.MIN_VALUE;
      this.distinctValues = 0;
    }

    public void updateStats(int value) {
      minValue = Math.min(minValue, value);
      maxValue = Math.max(maxValue, value);
    }

    public int getMinValue() { return minValue; }
    public int getMaxValue() { return maxValue; }
    public int getDistinctValues() {
      // As per spec, V(R,A) for base table is max - min + 1 assuming uniform distribution
      return maxValue - minValue + 1;
    }
  }

  // turn the for table into a line in the stats file
  public void serialize(DataOutputStream out) throws IOException {
  }

  // read a line from the stats file and populate the table stats
  public static TableStats deserialize(DataInputStream in) throws IOException {
  }


}
