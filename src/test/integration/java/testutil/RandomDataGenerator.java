package testutil;

import catalog.ColumnStats;
import catalog.DBCatalog;
import catalog.TableStats;
import io.writer.BinaryTupleWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;

public class RandomDataGenerator {
  private final Random random;
  private final List<ColumnStats> columnStatsCache;

  /**
   * Creates a new RandomDataGenerator for a given TableStats and schema
   *
   * @param tableStats The table statistics to use for data generation
   * @param schema The schema of the table
   */
  public RandomDataGenerator(TableStats tableStats, List<Column> schema) {
    this.random = new Random();
    this.columnStatsCache = new ArrayList<>(schema.size());

    // Cache column stats and precompute ranges during initialization
    for (Column column : schema) {
      String columnName = column.getColumnName();
      ColumnStats stats = tableStats.getColumnStats(columnName);

      if (stats == null) {
        throw new RuntimeException("Column " + columnName + " not found in table stats");
      }

      columnStatsCache.add(stats);
    }
  }

  public static void generateRandomTable(TableStats tablestats, String outputPath)
      throws IOException {
    RandomDataGenerator randomDataGenerator =
        new RandomDataGenerator(
            tablestats, DBCatalog.getInstance().getSchemaForTable(tablestats.getTableName()));

    try (FileChannel fileChannel = new FileOutputStream(outputPath).getChannel(); ) {
      BinaryTupleWriter writer = new BinaryTupleWriter(fileChannel, tablestats.getNumColumns());
      for (int i = 0; i < tablestats.getNumTuples(); i++) {
        Tuple tuple = randomDataGenerator.generateTuple();
        writer.writeTuple(tuple);
      }
      writer.close();
    }
  }

  /**
   * Generates a single random tuple based on the cached statistics
   *
   * @return A new Tuple with random values within the stats bounds
   */
  public Tuple generateTuple() {
    ArrayList<Integer> values = new ArrayList<>(columnStatsCache.size());

    for (int i = 0; i < columnStatsCache.size(); i++) {
      ColumnStats stats = columnStatsCache.get(i);
      // Generate random value using cached min value and precomputed range
      int randomValue =
          stats.getMinValue() + random.nextInt(stats.getMaxValue() - stats.getMinValue() + 1);
      values.add(randomValue);
    }

    return new Tuple(values);
  }

  /**
   * Generates multiple random tuples
   *
   * @param count Number of tuples to generate
   * @return List of generated tuples
   */
  public List<Tuple> generateTuples(int count) {
    List<Tuple> tuples = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      tuples.add(generateTuple());
    }
    return tuples;
  }

  /**
   * Sets the random seed for reproducible data generation
   *
   * @param seed The seed value
   */
  public void setSeed(long seed) {
    random.setSeed(seed);
  }
}
