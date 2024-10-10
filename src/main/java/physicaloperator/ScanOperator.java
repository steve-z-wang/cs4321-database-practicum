package physicaloperator;

import common.BinaryTupleReader;
import common.DBCatalog;
import common.Tuple;
import java.io.File;
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
  private BinaryTupleReader tupleReader;

  public ScanOperator(Table table) {
    super(null);

    this.outputSchema = new ArrayList<>();

    // Create a copy of the schema for the output schema
    ArrayList<Column> dbSchema = dbCatalog.getSchemaForTable(table.getName());
    for (Column dbSchemaColumn : dbSchema) {
      this.outputSchema.add(new Column(table, dbSchemaColumn.getColumnName()));
    }

    // get file
    file = dbCatalog.getFileForTable(table.getName());

    // create tuple reader
    try {
      tupleReader = new BinaryTupleReader(file.getAbsolutePath(), dbSchema.size());
    } catch (IOException e) {
      logger.error("Error creating BinaryTupleReader: ", e);
    }
  }

  @Override
  public void reset() {
    try {
      tupleReader.reset();
    } catch (IOException e) {
      logger.error("Error resetting BinaryTupleReader: ", e);
    }
  }

  @Override
  public Tuple getNextTuple() {
    Tuple toReturn = null;

    try {
      toReturn = tupleReader.getNextTuple();
    } catch (IOException e) {
      logger.error("Error reading next tuple in ScanOperator: ", e);
    }

    return toReturn;
  }
}
