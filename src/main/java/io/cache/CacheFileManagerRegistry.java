package io.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CacheFileManagerRegistry {
  private static Logger logger = LogManager.getLogger(CacheFileManagerRegistry.class);

  private final List<CacheFileManager> instances;
  private static CacheFileManagerRegistry registry;

  private String cacheDirectory;

  private CacheFileManagerRegistry() {
    instances = new ArrayList<>();
  }

  public static CacheFileManagerRegistry getInstance() {
    if (registry == null) {
      registry = new CacheFileManagerRegistry();
    }
    return registry;
  }

  public void setCacheDirectory(String cacheDirectory) {
    this.cacheDirectory = cacheDirectory;
  }

  public CacheFileManager createManager() throws IOException {
    CacheFileManager manager = new CacheFileManager(cacheDirectory);
    instances.add(manager);
    return manager;
  }

  public void cleanupAll() throws IOException {
    for (CacheFileManager manager : instances) {
      manager.cleanup();
    }
    instances.clear();
  }
}
