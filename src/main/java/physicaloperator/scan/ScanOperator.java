package physicaloperator.scan;

import config.DBCatalog;
import io.reader.BinaryTupleReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import physicaloperator.PhysicalOperator;

/** able to perform SELECT * FROM MyTable */
public class ScanOperator extends PhysicalOperator {

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
    try {
      FileChannel channel =
          new FileInputStream(dbCatalog.getTablePath(table.getName())).getChannel();
      tupleReader = new BinaryTupleReader(channel);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void reset() {
    try {
      tupleReader.reset();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Tuple getNextTuple() {
    try {
      return tupleReader.getNextTuple();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
