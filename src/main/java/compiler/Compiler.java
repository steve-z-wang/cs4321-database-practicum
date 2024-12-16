package compiler;

import catalog.DBCatalog;
import config.IndexConfigManager;
import config.InterpreterConfig;
import config.PhysicalPlanConfig;
// import index.IndexBuilder;
import index.IndexBuilder;
import io.cache.CacheFileManagerRegistry;
import io.writer.BinaryTupleWriter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
import queryplan.QueryPlanBuilder;

/**
 * Top level harness class; reads queries from an input file one at a time, processes them and sends
 * output to file or to System depending on flag.
 */
public class Compiler {
  private static final Logger logger = LogManager.getLogger();

  private static InterpreterConfig interpreterConfig;
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

    try {

      // Read interpreter config file
      String interpreterConfigFilePath = args[0];
      interpreterConfig = InterpreterConfig.getInstance();
      interpreterConfig.loadConfig(interpreterConfigFilePath);

      initializeDatabaseEnvironment();

      // Read and parse queries
      String str = Files.readString(Paths.get(interpreterConfig.getInputDir() + "/queries.sql"));
      Statements statements = CCJSqlParserUtil.parseStatements(str);

      // Run queries
      if (interpreterConfig.shuoldProcessQueries()) {
        cleanOutputDirectory();
        queryPlanBuilder = new QueryPlanBuilder();
        processQueries(statements);
      }

    } catch (Exception e) {
      System.err.println("Exception occurred in interpreter");
      logger.error(e.getStackTrace());
      throw new RuntimeException(e);
    }
  }

  public static void initializeDatabaseEnvironment() throws IOException {
    // Set up the database catalog
    DBCatalog.getInstance().setDataDirectory(interpreterConfig.getInputDir() + "/db");

    // Set up the index config
    IndexConfigManager.getInstance()
        .loadConfig(interpreterConfig.getInputDir() + "/db/index_info.txt");
    IndexConfigManager.getInstance().setIndexDir(interpreterConfig.getInputDir() + "/db/indexes");

    // Set up the physical plan config
    PhysicalPlanConfig.getInstance()
        .loadConfig(interpreterConfig.getInputDir() + "/plan_builder_config.txt");

    // Set up the cache directory
    CacheFileManagerRegistry.getInstance().setCacheDirectory(interpreterConfig.getTempDir());

    // Create Index
    if (interpreterConfig.shouldBuildIndex()) {
      IndexBuilder indexBuilder = new IndexBuilder();
      indexBuilder.buildIndexes();
    }
  }

  private static void cleanOutputDirectory() {
    if (outputToFiles) {
      for (File file : (new File(interpreterConfig.getOutputDir()).listFiles()))
        file.delete(); // clean output directory
    }
  }

  static void processQueries(Statements statements) {
    int counter = 1; // for numbering output files
    for (Statement statement : statements.getStatements()) {

      logger.info("Processing query: " + statement);

      try {
        processSingleQuery(statement, counter);
        CacheFileManagerRegistry.getInstance().cleanupAll();
      } catch (Exception e) {
        logger.error("Error processing query: " + statement, e);
      }

      ++counter;
    }
  }

  private static void processSingleQuery(Statement statement, int counter)
      throws ExecutionControl.NotImplementedException, IOException {
    PhysicalOperator plan = queryPlanBuilder.buildPlan(statement);

    String logicalStructure = queryPlanBuilder.getLogicalSctructure();
    String physicalStructure = queryPlanBuilder.getPhysicalSctructure();



    if (outputToFiles) {
      Path logicalOutPath = Paths.get(interpreterConfig.getOutputDir()).resolve("query" + counter+"_logicalplan");
      Path physicalOutPath = Paths.get(interpreterConfig.getOutputDir()).resolve("query" + counter+"_physicalplan");

      Path outfile = Paths.get(interpreterConfig.getOutputDir()).resolve("query" + counter);
      logger.info("Output file: {}", outfile);

      // Create the file if it doesn't exist
      if (!Files.exists(outfile)) {
        Files.createFile(outfile);
      }

      BinaryTupleWriter writer =
          new BinaryTupleWriter(outfile.toString(), plan.getOutputSchema().size());
      plan.dump(writer);

      try (BufferedWriter logicalWriter = Files.newBufferedWriter(logicalOutPath, StandardCharsets.UTF_8)) {
        logicalWriter.write(logicalStructure);
      }

      try (BufferedWriter physicalWriter = Files.newBufferedWriter(physicalOutPath, StandardCharsets.UTF_8)) {
        physicalWriter.write(physicalStructure);
      }

    } else {
      plan.dump(System.out);
    }
  }
}
