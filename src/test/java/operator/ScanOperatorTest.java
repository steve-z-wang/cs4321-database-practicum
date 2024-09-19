package operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import common.DBCatalog;
import common.Tuple;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for the ScanOperator. */
public class ScanOperatorTest {

  private Path tempDir;
  private DBCatalog dbCatalog;
  private Table table;
  private final String tableName = "TestTable";

  @BeforeEach
  public void setUp() throws IOException {
    // Create a temporary directory to act as the dbDirectory
    tempDir = Files.createTempDirectory("dbTestDir");

    // Create schema.txt file within the temporary directory
    File schemaFile = new File(tempDir.toFile(), "schema.txt");
    try (FileWriter schemaWriter = new FileWriter(schemaFile)) {
      schemaWriter.write(tableName + " col1 col2 col3\n");
    }

    // Create the data directory
    File dataDir = new File(tempDir.toFile(), "data");
    dataDir.mkdir();

    // Create the data file within the data directory
    File dataFile = new File(dataDir, tableName);
    try (FileWriter dataWriter = new FileWriter(dataFile)) {
      dataWriter.write("1,2,3\n");
      dataWriter.write("4,5,6\n");
      dataWriter.write("7,8,9\n");
    }

    // Initialize the DBCatalog and set the data directory
    dbCatalog = DBCatalog.getInstance();
    dbCatalog.setDataDirectory(tempDir.toString());

    // Create a Table object for the ScanOperator
    table = new Table();
    table.setName(tableName);
  }

  @AfterEach
  public void tearDown() throws IOException {
    // Delete the temporary directory and its contents
    Files.walk(tempDir).map(Path::toFile).forEach(File::delete);
  }

  @Test
  public void testScanOperatorReadsAllTuples() {

    ScanOperator scanOp = new ScanOperator(table);

    ArrayList<Tuple> tuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = scanOp.getNextTuple()) != null) {
      assertNotNull(tuple, "Tuple should not be null");
      tuples.add(tuple);
    }

    assertEquals(3, tuples.size(), "Should read three tuples");

    // Check content of tuples
    assertEquals("1,2,3", tuples.get(0).toString());
    assertEquals("4,5,6", tuples.get(1).toString());
    assertEquals("7,8,9", tuples.get(2).toString());
  }

  @Test
  public void testScanOperatorReset() {
    ScanOperator scanOp = new ScanOperator(table);

    // Read the first tuple
    Tuple firstTuple = scanOp.getNextTuple();
    assertEquals("1,2,3", firstTuple.toString());

    // Reset the operator
    scanOp.reset();

    // Read the first tuple again
    Tuple resetFirstTuple = scanOp.getNextTuple();
    assertEquals("1,2,3", resetFirstTuple.toString());
  }
}
