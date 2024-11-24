package integrationTest;

import static testUtil.HelperMethods.compareTupleListsAnyOrder;

import config.PhysicalPlanConfig.JoinMethod;
import config.PhysicalPlanConfig.SortMethod;
import java.io.IOException;
import java.util.List;
import model.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BNLJQueryTest extends BasicQueryTestLarge {
  private static final Logger logger = LogManager.getLogger(BNLJQueryTest.class);

  @Override
  void configureJoinAndSortMethods() {
    logger.info("Configuring BNLJ and in-memory sort");
    physicalPlanConfig.setJoinConfig(JoinMethod.BNLJ, 5);
    physicalPlanConfig.setSortConfig(SortMethod.IN_MEMORY, 0);
  }

  @Override
  protected void verifyQueryResults(List<Tuple> expectedTuples, List<Tuple> tuples)
      throws IOException {
    if (!compareTupleListsAnyOrder(expectedTuples, tuples)) {
      throw new AssertionError("Query results do not match");
    }
  }
}
