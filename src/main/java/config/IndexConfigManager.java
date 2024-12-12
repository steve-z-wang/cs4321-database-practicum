package config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Singleton class that holds the configuration for the index. */
public class IndexConfigManager {

  private final Logger logger = LogManager.getLogger(IndexConfigManager.class);

  private final HashMap<String, IndexDefinition> indexDefinitions;
  private static IndexConfigManager instance;

  private String indexDirectory;

  /** Reads the index configuration file and populates the index entries. */
  private IndexConfigManager() {
    indexDefinitions = new HashMap<>();
  }

  /**
   * Instance getter for singleton pattern, lazy initialization on first invocation.
   *
   * @return unique index configuration instance
   */
  public static IndexConfigManager getInstance() {
    if (instance == null) {
      instance = new IndexConfigManager();
    }
    return instance;
  }

  public String getIndexFilePath(IndexDefinition indexDefinition) {
    return String.format(
        "%s/%s.%s",
        this.indexDirectory, indexDefinition.getRelation(), indexDefinition.getAttribute());
  }

  public void setIndexDir(String indexDir) {
    indexDirectory = indexDir;
  }

  public IndexDefinition getIndexForRelation(String relation) {
    return indexDefinitions.get(relation);
  }

  public List<IndexDefinition> getIndexConfigs() {
    return new ArrayList<IndexDefinition>(indexDefinitions.values());
  }

  /**
   * Reads the index configuration file and populates the index entries.
   *
   * @param filePath Path to the index_info.txt file
   * @throws IOException if the file cannot be read
   */
  public void loadConfig(String filePath) throws IOException {
    try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(" ");
        if (parts.length != 4) {
          throw new IllegalArgumentException("Invalid index info format: " + line);
        }

        String relation = parts[0];
        String attribute = parts[1];
        boolean isClustered = parts[2].equals("1"); // unclustered (0) or clustered (1),
        int order = Integer.parseInt(parts[3]);

        indexDefinitions.put(
            relation, new IndexDefinition(relation, attribute, isClustered, order));
      }
    }
  }
}
