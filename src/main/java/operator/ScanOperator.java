package operator;

import common.DBCatalog;
import common.Tuple;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** able to perform SELECT * FROM MyTable */
public class ScanOperator extends Operator {

  private static final DBCatalog dbCatalog = DBCatalog.getInstance();
  private final Logger logger = LogManager.getLogger();

  private final File file;
  private BufferedReader reader;

  public ScanOperator(Table table) {
    super(null);

    this.outputSchema = new ArrayList<>();

    // Create output schema with table with alias
    ArrayList<Column> dbSchema = dbCatalog.getSchemaForTable(table.getName());
    for (Column dbSchemaColumn : dbSchema) {
      String columnName = dbSchemaColumn.getColumnName();
      Column outputSchemaColumn = new Column(table, columnName);
      this.outputSchema.add(outputSchemaColumn);
    }

    // get file
    file = dbCatalog.getFileForTable(table.getName());
    if (file == null) {
      logger.error("Data file for table " + table.getName() + " not found.");
    } else {
      setupReader();
    }
  }

  @Override
  public void reset() {
    closeReader();
    setupReader();
  }

  @Override
  public Tuple getNextTuple() {
    Tuple toReturn = null;

    try {
      String nextLine = reader != null ? reader.readLine() : null;
      if (nextLine == null) {
        closeReader();
      } else {
        toReturn = new Tuple(nextLine);
      }
    } catch (IOException e) {
      logger.error("Error reading next tuple in ScanOperator: ", e);
    }

    return toReturn;
  }

  private void setupReader() {
    if (file != null) {
      try {
        reader = new BufferedReader(new FileReader(file));
      } catch (FileNotFoundException e) {
        logger.error("File not found: " + file.getName(), e);
      }
    } else {
      logger.error("File is null, cannot set up reader.");
    }
  }

  private void closeReader() {
    if (reader != null) {
      try {
        reader.close();
        reader = null; // reset reader to null
      } catch (IOException e) {
        logger.error("Error closing reader in ScanOperator: ", e);
      }
    }
  }
}
