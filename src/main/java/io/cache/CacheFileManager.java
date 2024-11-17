package io.cache;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Manages cache files in an automatically created subdirectory. */
public class CacheFileManager implements FileManager {
  private static final Logger logger = LogManager.getLogger(CacheFileManager.class);
  private static final String BASE_DIRECTORY = "temp"; // or get from config

  private final Path directory;
  private final Set<String> activeFiles;

  public CacheFileManager() throws IOException {
    // Create unique subdirectory for this manager instance
    String uniqueDir = UUID.randomUUID().toString();
    this.directory = Path.of(BASE_DIRECTORY, uniqueDir);
    this.activeFiles = new HashSet<>();

    // Create directories
    Files.createDirectories(directory);
    logger.debug("Created cache directory: {}", directory);
  }

  // Alternative constructor to specify base directory
  public CacheFileManager(String baseDir) throws IOException {
    String uniqueDir = UUID.randomUUID().toString();
    this.directory = Path.of(baseDir, uniqueDir);
    this.activeFiles = new HashSet<>();

    Files.createDirectories(directory);
    logger.debug("Created cache directory: {}", directory);
  }

  @Override
  public FileChannel getWriteChannel(String fileName) throws IOException {
    Path filePath = directory.resolve(fileName);
    activeFiles.add(fileName);
    return FileChannel.open(
        filePath,
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.TRUNCATE_EXISTING);
  }

  @Override
  public FileChannel getReadChannel(String fileName) throws IOException {
    Path filePath = directory.resolve(fileName);
    return FileChannel.open(filePath, StandardOpenOption.READ);
  }

  @Override
  public void deleteFile(String fileName) throws IOException {
    Path filePath = directory.resolve(fileName);
    Files.deleteIfExists(filePath);
    activeFiles.remove(fileName);
  }

  @Override
  public void cleanup() throws IOException {
    // Delete all active files
    for (String fileName : activeFiles) {
      deleteFile(fileName);
    }

    // Delete directory
    if (Files.exists(directory)) {
      Files.walk(directory)
          .sorted((p1, p2) -> -p1.compareTo(p2)) // Reverse order to delete contents first
          .forEach(
              path -> {
                try {
                  Files.delete(path);
                } catch (IOException e) {
                  logger.warn("Failed to delete: {}", path, e);
                }
              });
    }
    logger.debug("Cleaned up directory: {}", directory);
  }

  // Getter for directory path (might be useful for testing)
  public Path getDirectory() {
    return directory;
  }
}
