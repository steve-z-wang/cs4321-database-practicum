import static testutil.HelperMethods.readBinary;

import config.PhysicalPlanConfig;
import io.cache.CacheFileManagerRegistry;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.IntStream;
import jdk.jshell.spi.ExecutionControl;
import model.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import testutil.QueryTestBase;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class P2BigQueryTest extends QueryTestBase {
  private static final Logger logger = LogManager.getLogger(P2BigQueryTest.class);
  PhysicalPlanConfig physicalPlanConfig = PhysicalPlanConfig.getInstance();

  @BeforeAll
  void setupLargeEnvironment() throws JSQLParserException, IOException, URISyntaxException {
    logger.info("Setting up large database environment");
    setupEnvironment("p2_large_query_test_samples");
    configureJoinAndSortMethods();
  }

  protected void configureJoinAndSortMethods() {
    PhysicalPlanConfig physicalPlanConfig = PhysicalPlanConfig.getInstance();
    physicalPlanConfig.setJoinMethod(PhysicalPlanConfig.JoinMethod.TNLJ, 0);
    physicalPlanConfig.setSortMethod(PhysicalPlanConfig.SortMethod.IN_MEMORY, 0);
  }

  @ParameterizedTest(name = "TNLJ & In-Memory Sort Query #{arguments}")
  @MethodSource("getAllTestCases")
  void testTNLJ(int queryIndex) throws ExecutionControl.NotImplementedException, IOException {

    physicalPlanConfig.setJoinMethod(PhysicalPlanConfig.JoinMethod.TNLJ, 0);
    physicalPlanConfig.setSortMethod(PhysicalPlanConfig.SortMethod.IN_MEMORY, 0);

    runTestByIndex(queryIndex);
  }

  @ParameterizedTest(name = "BNLJ 5 & In-Memory Sort Query #{arguments}")
  @MethodSource("getJoinTestCases")
  void testBNLJ(int queryIndex) throws ExecutionControl.NotImplementedException, IOException {

    physicalPlanConfig.setJoinMethod(PhysicalPlanConfig.JoinMethod.BNLJ, 5);
    physicalPlanConfig.setSortMethod(PhysicalPlanConfig.SortMethod.IN_MEMORY, 0);

    runTestByIndex(queryIndex);
  }

  @ParameterizedTest
  @MethodSource("getJoinTestCases")
  void testSMJ(int queryIndex) throws ExecutionControl.NotImplementedException, IOException {

    // set up
    physicalPlanConfig.setJoinMethod(PhysicalPlanConfig.JoinMethod.SMJ, 0);
    physicalPlanConfig.setSortMethod(PhysicalPlanConfig.SortMethod.EXTERNAL, 5);
    CacheFileManagerRegistry.getInstance().setCacheDirectory(baseDir + "/tempDir");

    runTestByIndex(queryIndex);
  }

  @ParameterizedTest
  @MethodSource("getJoinTestCases")
  private static IntStream getAllTestCases() {
    return IntStream.range(0, statementList.size());
  }

  private static IntStream getSortTestCases() {
    // TODO
    return null;
  }

  private static IntStream getJoinTestCases() {
    return IntStream.of(2, 3, 7, 8, 9, 11, 12, 13, 14);
  }

  @Override
  protected List<Tuple> getExpectedResult(int index) throws IOException {
    String expectedPath = baseDir + "/output_expected_binary/query" + (index + 1);
    return readBinary(expectedPath);
  }
}
