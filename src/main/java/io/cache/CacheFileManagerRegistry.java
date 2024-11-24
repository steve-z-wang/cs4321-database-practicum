package io.cache;

import config.PhysicalPlanConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CacheFileManagerRegistry {
  private static Logger logger = LogManager.getLogger(CacheFileManagerRegistry.class);
  private static final List<CacheFileManager> instances = new ArrayList<>();

  /**
   * Creates a new CacheFileManager instance and registers it.
   *
   * @return The newly created CacheFileManager instance
   * @throws IOException If an error occurs during creation
   */
  public static synchronized CacheFileManager createManager() throws IOException {

    PhysicalPlanConfig config = PhysicalPlanConfig.getInstance();
    String baseDir = config.getCacheDirectory();

    CacheFileManager manager = new CacheFileManager(baseDir);
    instances.add(manager);
    return manager;
  }

  /**
   * Cleans up all registered CacheFileManager instances.
   *
   * @throws IOException If an error occurs during cleanup
   */
  public static synchronized void cleanupAll() throws IOException {
    for (CacheFileManager manager : instances) {
      manager.cleanup();
    }
    instances.clear();
  }

  /**
   * Returns the list of all active CacheFileManager instances.
   *
   * @return List of CacheFileManager instances
   */
  public static synchronized List<CacheFileManager> getAllManagers() {
    return new ArrayList<>(instances);
  }
}
