package testUtil;

import static testUtil.HelperMethods.*;

import builder.QueryPlanBuilder;
import config.DBCatalog;
import config.PhysicalPlanConfig;
import io.cache.CacheFileManagerRegistry;
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
import org.junit.jupiter.api.AfterAll;
import physicaloperator.base.PhysicalOperator;

public abstract class QueryTestBase {
  private static final Logger logger = LogManager.getLogger(QueryTestBase.class);

  protected static String baseDir;
  protected static List<Statement> statementList;
  protected static QueryPlanBuilder queryPlanBuilder;
  protected static PhysicalPlanConfig physicalPlanConfig;

  protected static void setupEnvironment(String datasetSubDir)
      throws IOException, JSQLParserException, URISyntaxException {

    setupBaseDir(datasetSubDir);

    // Set up the database schema
    DBCatalog.getInstance().setDataDirectory(baseDir + "/input/db");

    // Parse the queries
    parseQueries();

    // Set up the query plan builder
    queryPlanBuilder = new QueryPlanBuilder();

    // Set up the physical plan config
    physicalPlanConfig = PhysicalPlanConfig.getInstance();
  }

  private static void setupBaseDir(String datasetDir) throws URISyntaxException {
    // Set up the base directory
    ClassLoader classLoader = QueryTestBase.class.getClassLoader();
    URI uri = Objects.requireNonNull(classLoader.getResource(datasetDir)).toURI();
    baseDir = new File(uri).getPath();
  }

  private static void parseQueries() throws IOException, JSQLParserException {
    String queriesContent = Files.readString(new File(baseDir + "/input/queries.sql").toPath());
    statementList = CCJSqlParserUtil.parseStatements(queriesContent).getStatements();
  }

  protected void runTestByIndex(int index)
      throws ExecutionControl.NotImplementedException, IOException {
    // Run the query
    List<Tuple> tuples = runQueryByIndex(index);

    // Get the expected results
    List<Tuple> expectedTuples = getExpectedResult(index);

    // Verify results
    verifyQueryResults(expectedTuples, tuples);
  }

  protected List<Tuple> runQueryByIndex(int index)
      throws ExecutionControl.NotImplementedException, IOException {
    Statement statement = statementList.get(index);
    logger.info("Running query: {}", statement);

    // Build the query plan & collect the results
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);
    return collectAllTuples(plan);
  }

  protected void runQueryByIndexWithNoCollect(int index)
      throws ExecutionControl.NotImplementedException, IOException {
    Statement statement = statementList.get(index);
    logger.info("Running query: {}", statement);

    // Build the query plan & collect the results
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);

    while (plan.getNextTuple() != null) {
      // Do nothing
    }
  }

  protected List<Tuple> getExpectedResult(int index)
      throws IOException, ExecutionControl.NotImplementedException {
    return null;
  }

  protected void verifyQueryResults(List<Tuple> expectedTuples, List<Tuple> tuples)
      throws IOException {
    if (!compareTupleListsExact(expectedTuples, tuples)) {
      throw new AssertionError("Query returned different results");
    }
  }

  @AfterAll
  public static void cleanup() throws IOException {
    CacheFileManagerRegistry.cleanupAll();
  }
}
