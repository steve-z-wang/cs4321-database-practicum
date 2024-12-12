package catalog;

import io.reader.BinaryTupleReader;
import net.sf.jsqlparser.schema.Column;
import org.apache.logging.log4j.core.tools.picocli.CommandLine;

import java.io.FileInputStream;
import java.nio.channels.FileChannel;

public class StatsCollector {


  public StatsCollector() {
    BinaryTupleReader binaryTupleReader = new BinaryTupleReader();
  }


  private void collectStats() {
    // TODO
    // 1. Read the schema file
    // 2. For each table, read the binary file
    // 3. Collect the statistics
    // 4. Store the statistics in a file

    for (String table : DBCatalog.getInstance().getTables()) {
      collectStatsForTable(table);
    }
  }

  private void collectStatsForTable(String table) {


    String binaryFile = DBCatalog.getInstance().getTablePath(table);
    FileChannel fileChannel = new FileInputStream(binaryFile).getChannel();
    BinaryTupleReader binaryTupleReader = new BinaryTupleReader(fileChannel);

    // Attribute name to column stats
    List<Column> schema = DBCatalog.getInstance().getSchemaForTable(table);

    List<Tuple> tuples = binaryTupleReader.readAllTuples();

    // TODO
    // 1. Create TableStats object
    // 2. loop through each tuple and update the stats object


  }

}
