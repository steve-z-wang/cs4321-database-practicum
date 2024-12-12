package config;

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PhysicalPlanConfig {

  private static final Logger logger = LogManager.getLogger(PhysicalPlanConfig.class);
  private static PhysicalPlanConfig instance;

  public enum JoinMethod {
    TNLJ,
    BNLJ,
    SMJ
  }

  public enum SortMethod {
    IN_MEMORY,
    EXTERNAL
  }

  public enum ScanMethod {
    INDEX_SCAN,
    FULL_SCAN
  }

  private JoinMethod joinMethod;
  private SortMethod sortMethod;
  private ScanMethod scanMethod;
  private int joinBufferPages;
  private int sortBufferPages;

  private PhysicalPlanConfig() {}

  public static PhysicalPlanConfig getInstance() {
    if (instance == null) {
      instance = new PhysicalPlanConfig();
    }
    return instance;
  }

  // Getters and setters

  public JoinMethod getJoinMethod() {
    return joinMethod;
  }

  public int getJoinBufferPages() {
    return joinBufferPages;
  }

  public SortMethod getSortMethod() {
    return sortMethod;
  }

  public int getSortBufferPages() {
    return sortBufferPages;
  }

  public ScanMethod getScanMethod() {
    return scanMethod;
  }

  public void setJoinMethod(JoinMethod method, int bufferPages) {
    if (method == JoinMethod.BNLJ && bufferPages <= 0) {
      throw new IllegalArgumentException("BNLJ requires positive buffer pages");
    }
    this.joinMethod = method;
    this.joinBufferPages = bufferPages;
  }

  public void setSortMethod(SortMethod method, int bufferPages) {
    if (method == SortMethod.EXTERNAL && bufferPages < 3) {
      throw new IllegalArgumentException("External sort requires at least 3 buffer pages");
    }
    this.sortMethod = method;
    this.sortBufferPages = bufferPages;
  }

  public void setScanMethod(ScanMethod method) {
    this.scanMethod = method;
  }

  // Load config from file

  public void loadConfig(String configFile) {
    logger.info("Reading config file: {}", configFile);
    try {
      BufferedReader br = new BufferedReader(new FileReader(configFile));
      String config = br.readLine() + "\n" + br.readLine();
      String[] lines = config.trim().split("\n");

      if (lines.length != 3) {
        throw new IllegalArgumentException("Configuration must contain exactly three lines");
      }

      parseJoinConfig(lines[0]);
      parseSortConfig(lines[1]);
      parseScanConfig(lines[2]);

      br.close();
    } catch (Exception e) {
      logger.error("Error reading config file: " + e.getMessage());
    }
    logger.info(
        "Join method: {}, Sort method: {}, Scan method: {}", joinMethod, sortMethod, scanMethod);
  }

  private void parseJoinConfig(String line) {
    String[] params = line.trim().split("\\s+");
    int methodCode = Integer.parseInt(params[0]);

    switch (methodCode) {
      case 0:
        setJoinMethod(JoinMethod.TNLJ, 0);
        break;
      case 1:
        if (params.length != 2) {
          throw new IllegalArgumentException("BNLJ requires buffer pages parameter");
        }
        setJoinMethod(JoinMethod.BNLJ, Integer.parseInt(params[1]));
        break;
      case 2:
        setJoinMethod(JoinMethod.SMJ, 0);
        break;
      default:
        throw new IllegalArgumentException("Invalid join method code: " + methodCode);
    }
  }

  private void parseSortConfig(String line) {
    String[] params = line.trim().split("\\s+");
    int methodCode = Integer.parseInt(params[0]);

    switch (methodCode) {
      case 0:
        setSortMethod(SortMethod.IN_MEMORY, 0);
        break;
      case 1:
        if (params.length != 2) {
          throw new IllegalArgumentException("External sort requires buffer pages parameter");
        }
        setSortMethod(SortMethod.EXTERNAL, Integer.parseInt(params[1]));
        break;
      default:
        throw new IllegalArgumentException("Invalid sort method code: " + methodCode);
    }
  }

  private void parseScanConfig(String line) {
    String[] params = line.trim().split("\\s+");
    int methodCode = Integer.parseInt(params[0]);

    switch (methodCode) {
      case 0:
        setScanMethod(ScanMethod.FULL_SCAN);
        break;
      case 1:
        setScanMethod(ScanMethod.INDEX_SCAN);
        break;
      default:
        throw new IllegalArgumentException("Invalid scan method code: " + methodCode);
    }
  }
}
