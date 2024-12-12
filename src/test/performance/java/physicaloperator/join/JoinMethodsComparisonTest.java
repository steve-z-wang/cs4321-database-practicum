package physicaloperator.join;

import static testutil.HelperMethods.convertToBinaryFiles;

import config.PhysicalPlanConfig.JoinMethod;
import config.PhysicalPlanConfig.SortMethod;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import testutil.QueryTestBase;

/**
 * In this test, we will compare the performance of 1. TNLJ 2. BNLJ with 1 buffer page 3. BNLJ with
 * 5 buffer pages 4. SMJ
 */
@Tag("performanceTest")
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class JoinMethodsComparisonTest extends QueryTestBase {
  private static final Logger logger = LogManager.getLogger(JoinMethodsComparisonTest.class);

  @BeforeAll
  static void setupJoinMethodsComparisonEnvironment()
      throws JSQLParserException, IOException, URISyntaxException {
    logger.info("Setting up join methods comparison environment");
    setupEnvironment("performance_test_samples");

    convertToBinaryFiles(baseDir + "/input/db/data_humanreadable", baseDir + "/input/db/data");
  }

  void configureJoinAndSortMethods(JoinMethod joinMethod, int bufferPages) {
    physicalPlanConfig.setJoinMethod(joinMethod, bufferPages);
    physicalPlanConfig.setSortMethod(SortMethod.EXTERNAL, 10);
  }

  @ParameterizedTest(name = "Query #{0} with {1} join method and {2} buffer pages")
  @MethodSource("queryArguments")
  void runQuery(int queryIndex, JoinMethod joinMethod, int bufferPages) throws Exception {
    logger.info(
        "Running query {} with {} join method and {} buffer pages",
        queryIndex,
        joinMethod,
        bufferPages);
    configureJoinAndSortMethods(joinMethod, bufferPages);

    // start the timer
    long startTime = System.currentTimeMillis();
    runQueryByIndexWithNoCollect(queryIndex);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    logger.info(
        "Query {} with {} join method and {} buffer pages took {} milliseconds",
        queryIndex,
        joinMethod,
        bufferPages,
        duration);
  }

  private static Stream<Arguments> queryArguments() {
    return IntStream.range(0, statementList.size())
        .boxed()
        .flatMap(
            queryIndex ->
                Stream.of(
                    Arguments.of(queryIndex, JoinMethod.TNLJ, 0),
                    Arguments.of(queryIndex, JoinMethod.BNLJ, 1),
                    Arguments.of(queryIndex, JoinMethod.BNLJ, 5),
                    Arguments.of(queryIndex, JoinMethod.SMJ, 0)));
  }
}
