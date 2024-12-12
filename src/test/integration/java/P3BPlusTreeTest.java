import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import config.IndexConfigManager;
import config.PhysicalPlanConfig;
import io.cache.CacheFileManagerRegistry;
import model.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import testutil.QueryTestBase;


public class P3BPlusTreeTest extends QueryTestBase {

  private static Logger logger = LogManager.getLogger(P3BPlusTreeTest.class);

  @BeforeAll
  static void setupLargeEnvironment() throws JSQLParserException, IOException, URISyntaxException {
    logger.info("Setting up large database environment");
    setupEnvironment("p3_b_plus_tree_test_samples");

    // Set up the physical plan config
    PhysicalPlanConfig.getInstance().loadConfig(baseDir + "/input/plan_builder_config.txt");

    // Set up the index config
    IndexConfigManager.getInstance().loadConfig(baseDir + "/input/db/index_info.txt");
    IndexConfigManager.getInstance().setIndexDir(baseDir + "/input/db/indexes");

  }

  @Override
  protected List<Tuple> getExpectedResult(int index) throws IOException {
    return List.of();
  }
}
