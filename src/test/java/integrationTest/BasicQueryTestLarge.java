package integrationTest;

import static testUtil.HelperMethods.readBinary;

import config.PhysicalPlanConfig;
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
import testUtil.QueryTestBase;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicQueryTestLarge extends QueryTestBase {
  private static final Logger logger = LogManager.getLogger(BasicQueryTestLarge.class);

  @BeforeAll
  void setupLargeEnvironment() throws JSQLParserException, IOException, URISyntaxException {
    logger.info("Setting up large database environment");
    setupEnvironment("integration_test_samples/large");
    configureJoinAndSortMethods();
  }

  void configureJoinAndSortMethods() {
    physicalPlanConfig.setJoinConfig(PhysicalPlanConfig.JoinMethod.TNLJ, 0);
    physicalPlanConfig.setSortConfig(PhysicalPlanConfig.SortMethod.IN_MEMORY, 0);
  }

  @ParameterizedTest(name = "Query #{arguments}")
  @MethodSource("queryIndices")
  void runQuery(int queryIndex) throws ExecutionControl.NotImplementedException, IOException {
    logger.info("Running query {}", queryIndex);
    runTestByIndex(queryIndex);
  }

  private static IntStream queryIndices() {
    return IntStream.range(0, statementList.size());
  }

  @Override
  protected List<Tuple> getExpectedResult(int index) throws IOException {
    String expectedPath = baseDir + "/output_expected_binary/query" + (index + 1);
    return readBinary(expectedPath);
  }
}
