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


    // TODO
    // Create test data set:
    //    1. generate 3 tables randomly each with 2 attribute
    //    2. create index config
    // Create test cases
    //    1. design 5 different test cases
    //    2. implement the expected results without using the B+ tree index
    //    3. run the index scan & compare the results

  }
}
