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

  /**
   * Open a file scan on the approprate data file
   *
   * @param outputSchema
   */
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

    // set up reader
    setupReader();
  }

  @Override
  public void reset() {
    closeReader();
    setupReader();
  }

  /**
   * Reads the next line from the file and returns the next tuple
   *
   * @return next Tuple, or null if we are at the end
   */
  @Override
  public Tuple getNextTuple() {

    Tuple toReturn = null;

    try {
      String nextLine = reader.readLine();
      if (nextLine == null) {
        closeReader();
      } else {
        toReturn = new Tuple(nextLine);
      }
    } catch (IOException e) {
      logger.error("Error reading next tuple in ScanOperator: ", e.getMessage());
    }

    return toReturn;
  }

  private void setupReader() {
    try {
      reader = new BufferedReader(new FileReader(file));
    } catch (FileNotFoundException e) {
      logger.error("File not found: ", e.getMessage());
    }
  }

  private void closeReader() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.error("Error closing reader in ScanOperator: ", e);
      }
    }
  }
}
