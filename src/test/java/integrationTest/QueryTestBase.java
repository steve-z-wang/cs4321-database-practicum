package integrationTest;

import static testUtil.HelperMethods.*;

import builder.QueryPlanBuilder;
import config.DBCatalog;
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
import physicaloperator.base.PhysicalOperator;

public abstract class QueryTestBase {
  private static final Logger logger = LogManager.getLogger(QueryTestBase.class);

  protected static String baseDir;
  protected static List<Statement> statementList;
  protected static QueryPlanBuilder queryPlanBuilder;

  void setupEnvironment(String datasetSubDir)
      throws IOException, JSQLParserException, URISyntaxException {
    // Set up the base directory
    ClassLoader classLoader = QueryTestBase.class.getClassLoader();
    URI uri =
        Objects.requireNonNull(classLoader.getResource("integration_test_samples/" + datasetSubDir))
            .toURI();
    baseDir = new File(uri).getPath();

    // Generate binary input files
    convertToBinaryFiles(baseDir + "/input/data_humanreadable", baseDir + "/input/data");

    // Set up the database schema
    DBCatalog.getInstance().setDataDirectory(baseDir + "/input/db");

    // Parse the queries
    String queriesContent = Files.readString(new File(baseDir + "/input/queries.sql").toPath());
    statementList = CCJSqlParserUtil.parseStatements(queriesContent).getStatements();
    queryPlanBuilder = new QueryPlanBuilder();
  }

  void runTestByIndex(int index) throws ExecutionControl.NotImplementedException, IOException {
    Statement statement = statementList.get(index);
    logger.info("Running query: {}", statement);

    // Build the query plan & collect the results
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);
    List<Tuple> tuples = collectAllTuples(plan);

    // Get the expected results
    List<Tuple> expectedTuples = getExpectedResult(index);

    // Verify results
    verifyQueryResults(expectedTuples, tuples);
  }

  abstract List<Tuple> getExpectedResult(int index) throws IOException;

  void verifyQueryResults(List<Tuple> expectedTuples, List<Tuple> tuples) throws IOException {
    if (!compareTupleListsExact(expectedTuples, tuples)) {
      throw new AssertionError("Query returned different results");
    }
  }
}
