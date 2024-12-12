// package physicaloperator.scan;
//
// import config.DBCatalog;
// import config.IndexConfigManager;
// import index.IndexBuilder;
// import java.io.File;
// import java.io.IOException;
// import java.net.URI;
// import java.net.URISyntaxException;
// import java.util.Objects;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.Test;
// import testUtil.QueryTestBase;
//
// public class IndexScanOperatorTest {
//
//   @BeforeAll
//   public static void setUpEnvironment() throws URISyntaxException, IOException {
//
//     // Set up the base directory
//     ClassLoader classLoader = QueryTestBase.class.getClassLoader();
//     URI uri =
//         Objects.requireNonNull(
//                 classLoader.getResource("integration_test_samples/index_scan_operator"))
//             .toURI();
//     String baseDir = new File(uri).getPath();
//
//     DBCatalog.getInstance().setDataDirectory(baseDir + "/input/db");
//     IndexConfigManager.getInstance().loadConfig(baseDir + "/input/db/index_info.txt");
//     IndexConfigManager.getInstance().setIndexDir(baseDir + "/input/db/indexes");
//   }
//
//   @Test
//   public void testIndexBuild() {
//
//     // Build indexes
//     IndexBuilder indexBuilder = new IndexBuilder();
//
//     // compare the output of the index build with the expected output
//     // assert that the output of the index build is the same as the expected output
//
//   }
// }
//
