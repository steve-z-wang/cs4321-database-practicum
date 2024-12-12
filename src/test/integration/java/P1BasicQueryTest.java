import static testutil.HelperMethods.convertToBinaryFiles;
import static testutil.HelperMethods.readHumanReadable;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.IntStream;
import jdk.jshell.spi.ExecutionControl;
import model.Tuple;
import net.sf.jsqlparser.JSQLParserException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import testutil.QueryTestBase;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class P1BasicQueryTest extends QueryTestBase {
  private static final Logger logger = LogManager.getLogger(P1BasicQueryTest.class);

  @BeforeAll
  void setUpSmallEnvironment() throws JSQLParserException, IOException, URISyntaxException {
    logger.info("Setting up small database environment");
    setupEnvironment("p1_small_query_test_samples");

    // Generate binary input files
    convertToBinaryFiles(baseDir + "/input/data_humanreadable", baseDir + "/input/data");
  }

  @ParameterizedTest(name = "Query #{arguments}")
  @MethodSource("queryIndices")
  void runQuery(int queryIndex) throws ExecutionControl.NotImplementedException, IOException {
    logger.info("Running query {}", queryIndex);
    runTestByIndex(queryIndex);
  }

  private static IntStream queryIndices() {
    return IntStream.range(0, statementList.size());
  }

  @Override
  protected List<Tuple> getExpectedResult(int index) throws IOException {
    String expectedPath = baseDir + "/output_expected_humanreadable/query" + (index + 1);
    return readHumanReadable(expectedPath);
  }
}
