package integration;

import config.PhysicalPlanConfig;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ExternalSortQueryTest extends QueryTestBase {
  private static final Logger logger = LogManager.getLogger(ExternalSortQueryTest.class);

  @BeforeAll
  void setUpConfig() {
    PhysicalPlanConfig.getInstance().setSortConfig(PhysicalPlanConfig.SortMethod.EXTERNAL, 3);
  }

  @ParameterizedTest(name = "External Sort Query #{arguments}")
  @MethodSource("queryIndices")
  void runExternalSortQuery(int queryIndex) throws Exception {
    logger.info("Running External Sort query {}", queryIndex);
    runTestByIndex(queryIndex);

  }

  private static IntStream queryIndices() {
    return IntStream.of(7, 8, 9, 18, 19, 20, 21, 22);
  }

}
