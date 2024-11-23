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
public class ExternalSortQueryTest extends BasicQueryTestLarge {
  private static final Logger logger = LogManager.getLogger(ExternalSortQueryTest.class);

  @Override
  void configureJoinAndSortMethods() {
      logger.info("Configuring TNLJ and external sort");
      PhysicalPlanConfig.getInstance().setJoinConfig(PhysicalPlanConfig.JoinMethod.TNLJ, 5);
      PhysicalPlanConfig.getInstance().setSortConfig(PhysicalPlanConfig.SortMethod.EXTERNAL, 3);
  }

  private static IntStream queryIndices() {
    return IntStream.of(2, 3, 4);
  }
}


