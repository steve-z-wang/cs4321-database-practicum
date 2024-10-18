import common.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import physicaloperator.Operator;

public class P1UnitTests {
  private static final Logger logger = LogManager.getLogger(P1UnitTests.class);

  private static Path expectedOutputPath;
  private static Path outputPath;
  private static List<Statement> statementList;
  private static QueryPlanBuilder queryPlanBuilder;

  @BeforeAll
  static void setupBeforeAllTests() throws IOException, JSQLParserException, URISyntaxException {
    ClassLoader classLoader = P1UnitTests.class.getClassLoader();
    URI uri = Objects.requireNonNull(classLoader.getResource("samples")).toURI();
    Path path = Paths.get(uri);
    Path inputPath = path.resolve("input");
    expectedOutputPath = path.resolve("expected_output");
    outputPath = path.resolve("output_human_readable");

    // Load DB schema, this read the schema.txt file
    DBCatalog.getInstance().setDataDirectory(inputPath.resolve("db").toString());

    // Generate binary input files
    HelperMethods.generateBinaryInputFile(inputPath.resolve("db"));

    // Load queries
    Path queriesFilePath = inputPath.resolve("queries.sql");
    Statements statements = CCJSqlParserUtil.parseStatements(Files.readString(queriesFilePath));
    statementList = statements.getStatements();

    // Initialize query plan builder
    queryPlanBuilder = new QueryPlanBuilder();
  }

  @Test
  public void runAllQueries() throws ExecutionControl.NotImplementedException {
    for (int i = 0; i < statementList.size(); i++) {
      runTestByIndex(i);
    }
  }

  @ParameterizedTest
  @MethodSource("queryIndices")
  public void runTestByIndex(int index) throws ExecutionControl.NotImplementedException {

    Statement statement = statementList.get(index);
    logger.info("Running query: " + statement);

    // build the query plan
    Operator plan = queryPlanBuilder.buildPlan(statement);
    logger.info("Query plan: " + plan);

    // run the query
    List<Tuple> tuples = HelperMethods.collectAllTuples(plan);
    logger.info("Number of rows: " + tuples.size());

    // write the output to a human readable file
    HelperMethods.writeOutput(outputPath, index, tuples);

    // get the expected results
    List<Tuple> expectedTuples = HelperMethods.getExpectedTuples(expectedOutputPath, index);

    // compare the results
    Assertions.assertEquals(expectedTuples.size(), tuples.size(), "Unexpected number of rows.");
    for (int i = 0; i < expectedTuples.size(); i++) {
      Tuple expectedTuple = expectedTuples.get(i);
      Tuple actualTuple = tuples.get(i);
      Assertions.assertEquals(expectedTuple, actualTuple, "Unexpected tuple at index " + i);
    }
  }

  static IntStream queryIndices() {
    return IntStream.range(0, statementList.size());
  }

  @Test
  public void runSingleTest() throws ExecutionControl.NotImplementedException {
    int index = 16; // specify the index of the test case you want to run
    runTestByIndex(index);
  }
}
