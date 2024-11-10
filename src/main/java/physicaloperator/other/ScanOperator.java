package physicaloperator.other;

import config.DBCatalog;
import io.reader.BinaryTupleReader;
import java.io.File;
import java.util.ArrayList;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import physicaloperator.base.Operator;

/** able to perform SELECT * FROM MyTable */
public class ScanOperator extends Operator {

  private static final DBCatalog dbCatalog = DBCatalog.getInstance();

  private final BinaryTupleReader tupleReader;

  public ScanOperator(Table table) {
    super(null);

    this.outputSchema = new ArrayList<>();

    // Create a copy of the schema for the output schema
    ArrayList<Column> dbSchema = dbCatalog.getSchemaForTable(table.getName());
    for (Column dbSchemaColumn : dbSchema) {
      this.outputSchema.add(new Column(table, dbSchemaColumn.getColumnName()));
    }

    // get file
    File file = dbCatalog.getFileForTable(table.getName());

    // create tuple reader
    tupleReader = new BinaryTupleReader(file.getAbsolutePath());
  }

  @Override
  public void reset() {
    tupleReader.reset();
  }

  @Override
  public Tuple getNextTuple() {
    return tupleReader.getNextTuple();
  }
}
