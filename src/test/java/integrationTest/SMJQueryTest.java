package integrationTest;

import config.PhysicalPlanConfig;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SMJQueryTest extends BasicQueryTestSmall {
  private static final Logger logger = LogManager.getLogger(SMJQueryTest.class);

  @Override
  void configureJoinAndSortMethods() {
    logger.info("Configuring SMJ and external sort");
    physicalPlanConfig.setJoinConfig(PhysicalPlanConfig.JoinMethod.SMJ, 0);
    physicalPlanConfig.setSortConfig(PhysicalPlanConfig.SortMethod.EXTERNAL, 3);
  }

  private static IntStream queryIndices() {
    return IntStream.range(0, statementList.size());
  }
}
