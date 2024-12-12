package testutil;

import static testutil.HelperMethods.*;

import config.DBCatalog;
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
import physicaloperator.PhysicalOperator;
import queryplan.QueryPlanBuilder;

public abstract class QueryTestBase {
  private static final Logger logger = LogManager.getLogger(QueryTestBase.class);

  protected static String baseDir;
  protected static List<Statement> statementList;
  protected static QueryPlanBuilder queryPlanBuilder;

  protected static void setupEnvironment(String datasetSubDir)
      throws IOException, JSQLParserException, URISyntaxException {

    // Set up the base directory
    ClassLoader classLoader = QueryTestBase.class.getClassLoader();
    URI uri = Objects.requireNonNull(classLoader.getResource(datasetSubDir)).toURI();
    baseDir = new File(uri).getPath();

    // Set up the database schema
    DBCatalog.getInstance().setDataDirectory(baseDir + "/input/db");

    // Parse the queries
    String queriesContent = Files.readString(new File(baseDir + "/input/queries.sql").toPath());
    statementList = CCJSqlParserUtil.parseStatements(queriesContent).getStatements();

    // Set up the query plan builder
    queryPlanBuilder = new QueryPlanBuilder();
  }

  protected void runTestByIndex(int index)
      throws ExecutionControl.NotImplementedException, IOException {
    logger.info("Running test for query {}", index + 1);

    // get the statement
    Statement statement = statementList.get(index);
    logger.info("Running query: {}", statement);

    // Build the query plan & collect the results
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);
    List<Tuple> tuples = collectAllTuples(plan);

    // Get the expected results
    logger.info("Getting expected results for query {}", index + 1);
    List<Tuple> expectedTuples = getExpectedResult(index);

    // Verify results
    logger.info("Verifying results for query {}", index + 1);

    verifyQueryResults(expectedTuples, tuples);
  }

  protected abstract List<Tuple> getExpectedResult(int index) throws IOException;

  protected void verifyQueryResults(List<Tuple> expectedTuples, List<Tuple> tuples)
      throws IOException {
    if (!compareTupleListsExact(expectedTuples, tuples)) {
      throw new AssertionError("Query returned different results");
    }
  }

  @AfterAll
  public static void cleanup() throws IOException {
    CacheFileManagerRegistry.getInstance().cleanupAll();
  }
}
