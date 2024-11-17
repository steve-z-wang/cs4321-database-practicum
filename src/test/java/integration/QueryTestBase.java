package integration;

import builder.QueryPlanBuilder;
import config.DBCatalog;
import io.reader.BinaryTupleReader;
import io.reader.HumanReadableTupleReader;
import io.writer.BinaryTupleWriter;
import io.writer.HumanReadableTupleWriter;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import jdk.jshell.spi.ExecutionControl;
import model.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import physicaloperator.base.PhysicalOperator;

import static util.HelperMethods.*;

public class QueryTestBase {
  private static final Logger logger = LogManager.getLogger(QueryTestBase.class);

  protected static String baseDir;
  protected static List<Statement> statementList;
  protected static QueryPlanBuilder queryPlanBuilder;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException, URISyntaxException {
    ClassLoader classLoader = QueryTestBase.class.getClassLoader();
    URI uri = Objects.requireNonNull(classLoader.getResource("sample-integration")).toURI();
    baseDir = new File(uri).getPath();

    generateBinaryInputFiles();
    setupDBAndQueries();
  }

  private static void setupDBAndQueries() throws IOException, JSQLParserException {
    DBCatalog.getInstance().setDataDirectory(baseDir + "/input/db");

    String queriesContent = Files.readString(new File(baseDir + "/input/queries.sql").toPath());
    statementList = CCJSqlParserUtil.parseStatements(queriesContent).getStatements();
    queryPlanBuilder = new QueryPlanBuilder();
  }

  private static void generateBinaryInputFiles() throws IOException {
    logger.info("Generating binary input files");
    File humanReadableDir = new File(baseDir + "/input/data_human_readable");
    File binaryDataDir = new File(baseDir + "/input/data");
    binaryDataDir.mkdirs();

    File[] files = humanReadableDir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          List<Tuple> tuples = readHumanReadable(file.getPath());
          writeBinary(new File(binaryDataDir, file.getName()).getPath(), tuples);
        }
      }
    }
  }

  protected void runTestByIndex(int index)
      throws ExecutionControl.NotImplementedException, IOException {
    Statement statement = statementList.get(index);
    logger.info("Running query: {}", statement);

    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);
    List<Tuple> tuples = collectAllTuples(plan);

    // Write both binary and human readable outputs
    String binaryOutPath = baseDir + "/output/query" + (index + 1);
    String hrOutPath = baseDir + "/output_human_readable/query" + (index + 1);

    writeBinary(binaryOutPath, tuples);
    writeHumanReadable(hrOutPath, tuples);

    // Verify results
    String expectedPath = baseDir + "/expected_output/query" + (index + 1);
    List<Tuple> expectedTuples = readHumanReadable(expectedPath);

    verifyQueryResults(expectedTuples, tuples);
  }

  protected void verifyQueryResults(List<Tuple> expected, List<Tuple> actual) {
    if (!compareTupleListsExact(expected, actual)) {
      throw new AssertionError("Query returned different results (ignoring order)");
    }
  }

  protected static List<Tuple> readBinary(String filePath) throws IOException {
    BinaryTupleReader reader = new BinaryTupleReader(filePath);
    List<Tuple> tuples = reader.getAllTuples();
    reader.close();
    return tuples;
  }

  protected static List<Tuple> readHumanReadable(String filePath) throws IOException {
    HumanReadableTupleReader reader = new HumanReadableTupleReader(filePath);
    List<Tuple> tuples = reader.getAllTuples();
    reader.close();
    return tuples;
  }

  protected static void writeBinary(String filePath, List<Tuple> tuples) throws IOException {
    if (!tuples.isEmpty()) {
      BinaryTupleWriter writer =
          new BinaryTupleWriter(filePath, tuples.getFirst().getAllElements().size());
      writer.writeTuples(tuples);
      writer.close();
    }
  }

  protected static void writeHumanReadable(String filePath, List<Tuple> tuples) throws IOException {
    if (!tuples.isEmpty()) {
      HumanReadableTupleWriter writer = new HumanReadableTupleWriter(filePath);
      writer.writeTuples(tuples);
      writer.close();
    }
  }

}
