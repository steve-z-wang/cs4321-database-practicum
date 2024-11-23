package integrationTest;

import config.PhysicalPlanConfig;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SMJQueryTest extends BasicQueryTestSmall {
  private static final Logger logger = LogManager.getLogger(SMJQueryTest.class);

  @BeforeAll
  void setupSMJConfig() {
    logger.info("Setting up SMJ config");
    PhysicalPlanConfig.getInstance().setJoinConfig(PhysicalPlanConfig.JoinMethod.SMJ, 0);
    PhysicalPlanConfig.getInstance().setSortConfig(PhysicalPlanConfig.SortMethod.EXTERNAL, 3);
    logger.info("Configured SMJ");
  }

  private static IntStream queryIndices() {
    return IntStream.range(0, statementList.size());
  }
}
