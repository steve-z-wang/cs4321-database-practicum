package config;

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PhysicalPlanConfig {
  private static final Logger logger = LogManager.getLogger(PhysicalPlanConfig.class);
  public static final int PAGE_SIZE = 4096;
  public static final int INT_SIZE = 4;

  public enum JoinMethod {
    TNLJ,
    BNLJ,
    SMJ
  }

  public enum SortMethod {
    IN_MEMORY,
    EXTERNAL
  }

  private static PhysicalPlanConfig instance;
  private JoinMethod joinMethod;
  private int joinBufferPages;
  private SortMethod sortMethod;
  private int sortBufferPages;
  private String configDirectory;

  private PhysicalPlanConfig() {
    joinMethod = JoinMethod.TNLJ;
    sortMethod = SortMethod.IN_MEMORY;
    joinBufferPages = 0;
    sortBufferPages = 0;
  }

  public static PhysicalPlanConfig getInstance() {
    if (instance == null) {
      instance = new PhysicalPlanConfig();
    }
    return instance;
  }

  /**
   * Sets the configuration directory and reads the config file
   *
   * @param configFile path of config file
   */
  public void setConfigFile(String configFile) {
    try {
      BufferedReader br = new BufferedReader(new FileReader(configFile));
      String config = br.readLine() + "\n" + br.readLine();
      parseConfig(config);
      br.close();
    } catch (Exception e) {
      logger.error("Error reading config file: " + e.getMessage());
    }
  }

  public void setJoinConfig(JoinMethod method, int bufferPages) {
    if (method == JoinMethod.BNLJ && bufferPages <= 0) {
      throw new IllegalArgumentException("BNLJ requires positive buffer pages");
    }
    this.joinMethod = method;
    this.joinBufferPages = bufferPages;
  }

  public void setSortConfig(SortMethod method, int bufferPages) {
    if (method == SortMethod.EXTERNAL && bufferPages < 3) {
      throw new IllegalArgumentException("External sort requires at least 3 buffer pages");
    }
    this.sortMethod = method;
    this.sortBufferPages = bufferPages;
  }

  private void parseConfig(String config) {
    String[] lines = config.trim().split("\n");
    if (lines.length != 2) {
      throw new IllegalArgumentException("Configuration must contain exactly two lines");
    }

    // Parse join method
    String[] joinParams = lines[0].trim().split("\\s+");
    int joinMethodCode = Integer.parseInt(joinParams[0]);

    switch (joinMethodCode) {
      case 0:
        setJoinConfig(JoinMethod.TNLJ, 0);
        break;
      case 1:
        if (joinParams.length != 2) {
          throw new IllegalArgumentException("BNLJ requires buffer pages parameter");
        }
        setJoinConfig(JoinMethod.BNLJ, Integer.parseInt(joinParams[1]));
        break;
      case 2:
        setJoinConfig(JoinMethod.SMJ, 0);
        break;
      default:
        throw new IllegalArgumentException("Invalid join method code: " + joinMethodCode);
    }

    // Parse sort method
    String[] sortParams = lines[1].trim().split("\\s+");
    int sortMethodCode = Integer.parseInt(sortParams[0]);

    switch (sortMethodCode) {
      case 0:
        setSortConfig(SortMethod.IN_MEMORY, 0);
        break;
      case 1:
        if (sortParams.length != 2) {
          throw new IllegalArgumentException("External sort requires buffer pages parameter");
        }
        setSortConfig(SortMethod.EXTERNAL, Integer.parseInt(sortParams[1]));
        break;
      default:
        throw new IllegalArgumentException("Invalid sort method code: " + sortMethodCode);
    }
  }

  // Getters
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
}
