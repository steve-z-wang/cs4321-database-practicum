package integration;

import config.PhysicalPlanConfig;
import java.io.IOException;
import java.util.stream.IntStream;
import jdk.jshell.spi.ExecutionControl;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BasicQueryTest extends QueryTestBase {
  private static final Logger logger = LogManager.getLogger(BasicQueryTest.class);

  @BeforeAll
  void setupBasicConfig() {
    logger.info("Configured Basic Query");
    PhysicalPlanConfig.getInstance().setJoinConfig(PhysicalPlanConfig.JoinMethod.TNLJ, 0);
    PhysicalPlanConfig.getInstance().setSortConfig(PhysicalPlanConfig.SortMethod.IN_MEMORY, 0);
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

  // @Test
  // public void runSingleTest() throws ExecutionControl.NotImplementedException, IOException {
  //   int index = 16; // specify the index of the test case you want to run
  //   runTestByIndex(index);
  // }
}
