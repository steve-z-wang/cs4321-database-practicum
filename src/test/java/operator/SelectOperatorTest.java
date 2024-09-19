package operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import common.DBCatalog;
import common.Tuple;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for the SelectOperator. */
public class SelectOperatorTest {

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
  public void testSelectOperatorWithMatchingCondition() {
    // Initialize the ScanOperator
    ScanOperator scanOp = new ScanOperator(table);

    // Define a simple WHERE condition: col1 = 4
    EqualsTo equalsTo = new EqualsTo();
    equalsTo.setLeftExpression(new Column(table, "col1"));
    equalsTo.setRightExpression(new LongValue(4));

    // Create SelectOperator with the expression
    SelectOperator selectOp = new SelectOperator(scanOp, equalsTo);

    ArrayList<Tuple> tuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = selectOp.getNextTuple()) != null) {
      tuples.add(tuple);
    }

    // Expect only the tuple "4,5,6" to match the condition col1 = 4
    assertEquals(1, tuples.size(), "Should read one tuple matching the condition");
    assertEquals("4,5,6", tuples.get(0).toString());
  }

  @Test
  public void testSelectOperatorWithNoMatchingCondition() {
    // Initialize the ScanOperator
    ScanOperator scanOp = new ScanOperator(table);

    // Define a condition that does not match any tuple: col1 = 10
    EqualsTo equalsTo = new EqualsTo();
    equalsTo.setLeftExpression(new Column(table, "col1"));
    equalsTo.setRightExpression(new LongValue(10));

    // Create SelectOperator with the expression
    SelectOperator selectOp = new SelectOperator(scanOp, equalsTo);

    // Ensure no tuples are returned
    Tuple tuple = selectOp.getNextTuple();
    assertNull(tuple, "No tuple should match the condition col1 = 10");
  }

  @Test
  public void testSelectOperatorWithNullCondition() {
    // Initialize the ScanOperator
    ScanOperator scanOp = new ScanOperator(table);

    // Create SelectOperator with null condition (should select all tuples)
    SelectOperator selectOp = new SelectOperator(scanOp, null);

    ArrayList<Tuple> tuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = selectOp.getNextTuple()) != null) {
      tuples.add(tuple);
    }

    // Expect all tuples to be returned
    assertEquals(3, tuples.size(), "Should read all three tuples when no condition is applied");
    assertEquals("1,2,3", tuples.get(0).toString());
    assertEquals("4,5,6", tuples.get(1).toString());
    assertEquals("7,8,9", tuples.get(2).toString());
  }

  @Test
  public void testSelectOperatorReset() {
    // Initialize the ScanOperator
    ScanOperator scanOp = new ScanOperator(table);

    // Define a simple WHERE condition: col1 = 4
    EqualsTo equalsTo = new EqualsTo();
    equalsTo.setLeftExpression(new Column(table, "col1"));
    equalsTo.setRightExpression(new LongValue(4));

    // Create SelectOperator with the expression
    SelectOperator selectOp = new SelectOperator(scanOp, equalsTo);

    // Read the first matching tuple
    Tuple firstTuple = selectOp.getNextTuple();
    assertEquals("4,5,6", firstTuple.toString());

    // Reset the operator
    selectOp.reset();

    // Read the first matching tuple again after reset
    Tuple resetFirstTuple = selectOp.getNextTuple();
    assertEquals("4,5,6", resetFirstTuple.toString());
  }
}
