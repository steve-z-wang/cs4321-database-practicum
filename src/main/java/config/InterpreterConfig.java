package config;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Singleton class for interpreter configuration */
public class InterpreterConfig {

  private static final Logger logger = LogManager.getLogger(InterpreterConfig.class);

  private static InterpreterConfig instance;

  private String inputDir;
  private String outputDir;
  private String tempDir;

  // whether to build index
  private Boolean shouldBuildIndex;
  private Boolean shuoldProcessQueries;

  private InterpreterConfig() {}

  public static InterpreterConfig getInstance() {
    if (instance == null) {
      instance = new InterpreterConfig();
    }
    return instance;
  }

  public void loadConfig(String configFile) {
    logger.info("Reading config file: {}", configFile);
    try {
      BufferedReader br = new BufferedReader(new FileReader(configFile));

      // read all five lines of the config file
      inputDir = br.readLine();
      outputDir = br.readLine();
      tempDir = br.readLine();
      shouldBuildIndex = Integer.parseInt(br.readLine()) == 1;
      shuoldProcessQueries = Integer.parseInt(br.readLine()) == 1;

    } catch (FileNotFoundException e) {
      logger.error("Config file not found: {}", configFile);
    } catch (IOException e) {
      logger.error("Error reading config file: {}", configFile);
    }
  }

  // Getters

  public String getInputDir() {
    return inputDir;
  }

  public String getOutputDir() {
    return outputDir;
  }

  public String getTempDir() {
    return tempDir;
  }

  public Boolean shouldBuildIndex() {
    return shouldBuildIndex;
  }

  public Boolean shuoldProcessQueries() {
    return shuoldProcessQueries;
  }
}
