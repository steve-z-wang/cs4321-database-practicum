import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import catalog.DBCatalog;
import config.IndexConfigManager;
import config.InterpreterConfig;
import config.PhysicalPlanConfig;
import index.IndexBuilder;
import io.cache.CacheFileManagerRegistry;
import jdk.jshell.spi.ExecutionControl;
import model.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import physicaloperator.PhysicalOperator;
import queryplan.QueryPlanBuilder;
import testutil.QueryTestBase;

import static testutil.HelperMethods.*;


public class P3BPlusTreeTest {

  private static Logger logger = LogManager.getLogger(P3BPlusTreeTest.class);

  static String baseDir;
  static List<Statement> statementList;
  static QueryPlanBuilder queryPlanBuilder;

  @BeforeAll
  public static void setupConfig() throws JSQLParserException, IOException, URISyntaxException {
    logger.info("Setting up large database environment");

    // // Generate random data
    // TableStats tablestats = TableStats.deserialize("Reserves 10000 G,0,10000 H,0,10000 I,0,10000");
    // RandomDataGenerator.generateRandomTable(tablestats, baseDir + "/input/db/data/" + tablestats.getTableName());

    // Set up the base directory
    ClassLoader classLoader = QueryTestBase.class.getClassLoader();
    URI uri = Objects.requireNonNull(classLoader.getResource("p3_b_plus_tree_test_samples")).toURI();
    baseDir = new File(uri).getPath();

    initializeDatabaseEnvironment();

    // Build the indexes
    IndexBuilder indexBuilder = new IndexBuilder();
    indexBuilder.buildIndexes();

    // Parse the queries
    String queriesContent = Files.readString(new File(baseDir + "/input/queries.sql").toPath());
    statementList = CCJSqlParserUtil.parseStatements(queriesContent).getStatements();

    // Set up the query plan builder
    queryPlanBuilder = new QueryPlanBuilder();
  }

  @ParameterizedTest(name = "Query #{arguments}")
  @MethodSource("queryIndices")
  void testIndexScan(int queryIndex) throws ExecutionControl.NotImplementedException, IOException {
    logger.info("Running query {}", queryIndex);
    runTestByIndex(queryIndex);
  }

  private static IntStream queryIndices() {
    return IntStream.range(0, statementList.size());
  }

  protected void runTestByIndex(int index)
      throws ExecutionControl.NotImplementedException, IOException {
    logger.info("Running test for query {}", index + 1);

    // get the statement
    Statement statement = statementList.get(index);

    // Get the results
    List<Tuple> tuples = getResult(statement);

    // Get the expected results
    List<Tuple> expectedTuples = getExpectedResult(statement);

    // Verify results
    logger.info("Verifying results for query {}", index + 1);
    if (!compareTupleListsAnyOrder(expectedTuples, tuples)) {
      throw new AssertionError("Query returned different results");
    }
  }

  private List<Tuple> getResult(Statement statement) throws IOException, ExecutionControl.NotImplementedException {
    PhysicalPlanConfig.getInstance().setScanMethod(PhysicalPlanConfig.ScanMethod.INDEX_SCAN);
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);
    return collectAllTuples(plan);
  }

  private List<Tuple> getExpectedResult(Statement statement) throws IOException, ExecutionControl.NotImplementedException {
    PhysicalPlanConfig.getInstance().setScanMethod(PhysicalPlanConfig.ScanMethod.FULL_SCAN);
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);
    return collectAllTuples(plan);
  }

  @Test
  public void testSingleCase() throws ExecutionControl.NotImplementedException, IOException {
    runTestByIndex(15);
  }

  public static void initializeDatabaseEnvironment() throws IOException {

    InterpreterConfig interpreterConfig = InterpreterConfig.getInstance();

    String inputDir = baseDir + "/input";
    String tempDir = "/temp";

    // Set up the database catalog
    DBCatalog.getInstance().setDataDirectory(inputDir + "/db");

    // Set up the index config
    IndexConfigManager.getInstance().loadConfig(inputDir + "/db/index_info.txt");
    IndexConfigManager.getInstance().setIndexDir(inputDir + "/db/indexes");

    // Set up the physical plan config
    PhysicalPlanConfig.getInstance().loadConfig(inputDir + "/plan_builder_config.txt");

    // Set up the cache directory
    CacheFileManagerRegistry.getInstance().setCacheDirectory(tempDir);
  }
}
