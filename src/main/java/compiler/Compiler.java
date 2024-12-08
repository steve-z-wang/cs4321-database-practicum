package compiler;

import builder.QueryPlanBuilder;
import config.DBCatalog;
import config.PhysicalPlanConfig;
import io.cache.CacheFileManagerRegistry;
import io.writer.BinaryTupleWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import jdk.jshell.spi.ExecutionControl;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.PhysicalOperator;

/**
 * Top level harness class; reads queries from an input file one at a time, processes them and sends
 * output to file or to System depending on flag.
 */
public class Compiler {
  private static final Logger logger = LogManager.getLogger();

  private static String outputDir;
  private static String inputDir;
  private static String cacheDir;
  private static final boolean outputToFiles = true; // true = output to
  private static QueryPlanBuilder queryPlanBuilder;

  // files, false = output
  // to System.out

  /**
   * Reads statements from queriesFile one at a time, builds query plan and evaluates, dumping
   * results to files or console as desired.
   *
   * <p>If dumping to files result of ith query is in file named queryi, indexed stating at 1.
   */
  public static void main(String[] args) {

    inputDir = args[0];
    outputDir = args[1];
    cacheDir = args[2];
    DBCatalog.getInstance().setDataDirectory(inputDir + "/db");
    PhysicalPlanConfig.getInstance().setConfigFile(inputDir + "/plan_builder_config.txt");
    PhysicalPlanConfig.getInstance().setCacheDirectory(cacheDir);

    try {
      // Read and parse queries
      String str = Files.readString(Paths.get(inputDir + "/queries.sql"));
      Statements statements = CCJSqlParserUtil.parseStatements(str);

      queryPlanBuilder = new QueryPlanBuilder();

      cleanOutputDirectory();
      processQueries(statements);

    } catch (Exception e) {
      System.err.println("Exception occurred in interpreter");
      logger.error(e.getMessage());
    }
  }

  private static void cleanOutputDirectory() {
    if (outputToFiles) {
      for (File file : (new File(outputDir).listFiles())) file.delete(); // clean output directory
    }
  }

  private static void processQueries(Statements statements) {
    int counter = 1; // for numbering output files
    for (Statement statement : statements.getStatements()) {

      logger.info("Processing query: " + statement);

      try {
        processSingleQuery(statement, counter);
        CacheFileManagerRegistry.cleanupAll();
      } catch (Exception e) {
        logger.error("Error processing query: " + statement, e);
      }

      ++counter;
    }
  }

  private static void processSingleQuery(Statement statement, int counter)
      throws ExecutionControl.NotImplementedException, IOException {
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);

    if (outputToFiles) {
      Path outfile = Paths.get(outputDir).resolve("query" + counter);
      logger.info("Output file: {}", outfile);

      // Create the file if it doesn't exist
      if (!Files.exists(outfile)) {
        Files.createFile(outfile);
      }

      BinaryTupleWriter writer =
          new BinaryTupleWriter(outfile.toString(), plan.getOutputSchema().size());
      plan.dump(writer);

    } else {
      plan.dump(System.out);
    }
  }
}
