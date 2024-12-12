package config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Singleton class that holds the configuration for the index. */
public class IndexConfigManager {

  private static IndexConfigManager instance;
  private final List<IndexDefinition> indexDefinitions = new ArrayList<>();
  private String INDEX_DIR;

  private IndexConfigManager() {}

  public static IndexConfigManager getInstance() {
    if (instance == null) {
      instance = new IndexConfigManager();
    }
    return instance;
  }

  public void setIndexDir(String indexDir) {
    INDEX_DIR = indexDir;
  }

  public String getIndexDir() {
    return INDEX_DIR;
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

        // unclustered (0) or clustered (1),
        boolean isClustered = parts[2].equals("1");
        int order = Integer.parseInt(parts[3]);
        indexDefinitions.add(new IndexDefinition(relation, attribute, isClustered, order));
      }
    }
  }

  /**
   * Returns the list of index entries.
   *
   * @return List of index entries
   */
  public List<IndexDefinition> getIndexConfigs() {
    return Collections.unmodifiableList(indexDefinitions);
  }
}
