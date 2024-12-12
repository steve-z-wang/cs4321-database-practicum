package catalog;

import java.io.BufferedWriter;
import java.io.IOException;

/** Inner class to hold statistics for a single column */
class ColumnStats {
  private int minValue;
  private int maxValue;
  private int distinctValues; // V-value as described in the project spec

  public ColumnStats() {
    this.minValue = Integer.MAX_VALUE;
    this.maxValue = Integer.MIN_VALUE;
    this.distinctValues = 0;
  }

  /**
   * Update the column statistics based on a new value
   *
   * @param value New value to update the statistics
   */
  public void updateStats(int value) {
    minValue = Math.min(minValue, value);
    maxValue = Math.max(maxValue, value);
  }

  public int getMinValue() {
    return minValue;
  }

  public int getMaxValue() {
    return maxValue;
  }

  public int getDistinctValues() {
    // As per spec, V(R,A) for base table is max - min + 1 assuming uniform distribution
    return maxValue - minValue + 1;
  }
}
