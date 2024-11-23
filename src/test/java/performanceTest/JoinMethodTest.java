package performanceTest;

import static testUtil.HelperMethods.convertToBinaryFiles;

import config.DBCatalog;
import integrationTest.QueryTestBase;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;

/**
 * In this test, we will compare the performance of 1. TNLJ 2. BNLJ with 1 buffer page 3. BNLJ with
 * 5 buffer pages 4. SMJ
 */
public class JoinMethodTest {
  private static Logger logger = LogManager.getLogger(JoinMethodTest.class);

  private static String baseDir;

  @BeforeAll
  static void setup() throws IOException, URISyntaxException {
    // Get the base directory
    ClassLoader classLoader = QueryTestBase.class.getClassLoader();
    URI uri = Objects.requireNonNull(classLoader.getResource("performance_test_samples")).toURI();
    baseDir = new File(uri).getPath();

    // Generate binary input files
    convertToBinaryFiles(baseDir + "/input/data_human_readable", baseDir + "/input/data");

    // Set up the database schema
    DBCatalog.getInstance().setDataDirectory(baseDir + "/input/db");

    //

  }
}
