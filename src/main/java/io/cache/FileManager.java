package io.cache;

import java.io.IOException;
import java.nio.channels.FileChannel;

public interface FileManager {
  // Returns FileChannel directly for better performance
  FileChannel getWriteChannel(String fileName) throws IOException;

  FileChannel getReadChannel(String fileName) throws IOException;

  void deleteFile(String fileName) throws IOException;

  void cleanup() throws IOException;
}
