package catalog;

import io.reader.BinaryTupleReader;
import java.io.*;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class StatsCollector {
  Logger logger = LogManager.getLogger(StatsCollector.class);
  private static StatsCollector instance;

  private static final String STATS_FILE = "stats.txt";
  private static String statsDirectory;

  private final HashMap<String, TableStats> tableStats;

  public static StatsCollector getInstance() {
    if (instance == null) {
      instance = new StatsCollector();
    }
    return instance;
  }

  private StatsCollector() {
    tableStats = new HashMap<>();
  }

  public void setStatsDirectory(String statsDirectory) {
    this.statsDirectory = statsDirectory;
  }

  /** Collects statistics for all tables in the database */
  public void collectStats() throws IOException {
    logger.info("Collecting stats for all tables");
    for (String table : DBCatalog.getInstance().getTables()) {
      TableStats tableStats = collectStatsForTable(table);
      this.tableStats.put(table, tableStats);
    }
  }

  public void writeStats() {
    logger.info("Writing stats to file");
    if (statsDirectory == null) {
      throw new RuntimeException("Stats directory not set");
    }

    try (BufferedWriter writer =
        new BufferedWriter(new FileWriter(statsDirectory + "/" + STATS_FILE))) {
      for (Map.Entry<String, TableStats> entry : tableStats.entrySet()) {
        TableStats tableStats = entry.getValue();
        tableStats.serialize(writer, DBCatalog.getInstance().getSchemaForTable(entry.getKey()));
      }
    } catch (IOException e) {
      logger.error("Error writing stats to file", e);
    }
  }

  /** Load table stats from the stats file */
  public void loadStats() {
    // TODO
  }

  private TableStats collectStatsForTable(String table) throws IOException {
    logger.info("Collecting stats for table " + table);
    String binaryFile = DBCatalog.getInstance().getTablePath(table);
    try (FileChannel fileChannel = new FileInputStream(binaryFile).getChannel(); ) {
      BinaryTupleReader binaryTupleReader = new BinaryTupleReader(fileChannel);

      TableStats tableStats = new TableStats(table);

      List<Column> schema = DBCatalog.getInstance().getSchemaForTable(table);

      Tuple tuple;
      while ((tuple = binaryTupleReader.getNextTuple()) != null) {

        System.out.println("Tuple: " + tuple);

        tableStats.updateStats(schema, tuple);
        tableStats.incrementTupleCount();
      }

      return tableStats;
    }
  }
}
