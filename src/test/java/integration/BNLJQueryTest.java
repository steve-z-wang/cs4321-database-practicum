package integration;

import config.PhysicalPlanConfig;
import config.PhysicalPlanConfig.JoinMethod;
import config.PhysicalPlanConfig.SortMethod;

import java.util.List;
import java.util.stream.IntStream;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static util.HelperMethods.compareTupleListsAnyOrder;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BNLJQueryTest extends QueryTestBase {
  private static final Logger logger = LogManager.getLogger(BNLJQueryTest.class);

  @BeforeAll
  void setupBNLJConfig() {
    PhysicalPlanConfig.getInstance().setJoinConfig(JoinMethod.BNLJ, 3);
    PhysicalPlanConfig.getInstance().setSortConfig(SortMethod.IN_MEMORY, 0);
    logger.info("Configured BNLJ with 3 buffer pages");
  }

  @Override
  protected void verifyQueryResults(List<Tuple> expected, List<Tuple> actual) {
    if (!compareTupleListsAnyOrder(expected, actual)) {
        throw new AssertionError("Query results do not match");
    }
  }

  @ParameterizedTest(name = "BNLJ Query #{arguments}")
  @MethodSource("queryIndices")
  void runBNLJQuery(int queryIndex) throws Exception {
    logger.info("Running BNLJ query {}", queryIndex);
    runTestByIndex(queryIndex);
  }

  private static IntStream queryIndices() {
    return IntStream.range(0, statementList.size());
  }
}
