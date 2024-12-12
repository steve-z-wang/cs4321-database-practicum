package index;

import static org.junit.jupiter.api.Assertions.assertEquals;

import config.DBCatalog;
import config.IndexConfigManager;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import testutil.QueryTestBase;
import utils.DBConstants;

public class IndexBuilderTest {

  private static String baseDir;
  private static IndexBuilder indexBuilder;

  @BeforeAll
  public static void setUpEnvironment() throws URISyntaxException, IOException {

    // Set up the base directory
    ClassLoader classLoader = QueryTestBase.class.getClassLoader();
    URI uri =
        Objects.requireNonNull(classLoader.getResource("p3_b_plus_tree_test_samples")).toURI();
    baseDir = new File(uri).getPath();

    DBCatalog.getInstance().setDataDirectory(baseDir + "/input/db");
    IndexConfigManager.getInstance().loadConfig(baseDir + "/input/db/index_info.txt");
    IndexConfigManager.getInstance().setIndexDir(baseDir + "/input/db/indexes");

    // build indexes
    indexBuilder = new IndexBuilder();
    indexBuilder.buildIndexes();
  }

  @Test
  public void testIndexBuild1() throws IOException {

    String expectedIndexPath = baseDir + "/expected_indexes/Boats.E";
    String indexPath = baseDir + "/input/db/indexes/Boats.E";

    compareIndexTrees(expectedIndexPath, indexPath);
  }

  @Test
  public void testIndexBuild2() throws IOException {

    String expectedIndexPath = baseDir + "/expected_indexes/Sailors.A";
    String indexPath = baseDir + "/input/db/indexes/Sailors.A";

    compareIndexTrees(expectedIndexPath, indexPath);
  }

  private static void compareIndexTrees(String expectedIndexPath, String indexPath)
      throws IOException {
    // read the index file
    FileChannel expectedIndexChannel = new FileInputStream(expectedIndexPath).getChannel();
    ByteBuffer expectedIndexBuffer = ByteBuffer.allocate(DBConstants.INDEX_PAGE_SIZE);

    FileChannel indexChannel = new FileInputStream(indexPath).getChannel();
    ByteBuffer indexBuffer = ByteBuffer.allocate(DBConstants.INDEX_PAGE_SIZE);

    // compare header
    BPlusTree expectedBPlusTree = BPlusTree.deserialize(expectedIndexChannel, expectedIndexBuffer);
    BPlusTree bPlusTree = BPlusTree.deserialize(indexChannel, indexBuffer);

    // assert that the output of the index build with the expected output
    assertEquals(expectedBPlusTree.getRootAddress(), bPlusTree.getRootAddress());
    assertEquals(expectedBPlusTree.getNumLeafNodes(), bPlusTree.getNumLeafNodes());
    assertEquals(expectedBPlusTree.getOrder(), bPlusTree.getOrder());

    // check each leaf node
    int pageAddress = 1;
    for (int i = 0; i < bPlusTree.getNumLeafNodes(); i++) {
      BPlusTreeLeafNode expectedLeaf =
          expectedBPlusTree.loadLeafNode(expectedIndexChannel, expectedIndexBuffer, pageAddress);
      BPlusTreeLeafNode leaf = bPlusTree.loadLeafNode(indexChannel, indexBuffer, pageAddress);
      compareLeafNodes(leaf, expectedLeaf);
      pageAddress++;
    }

    while (pageAddress < bPlusTree.getRootAddress()) {
      BPlusTreeIndexNode expectedindex =
          expectedBPlusTree.loadIndexNode(expectedIndexChannel, expectedIndexBuffer, pageAddress);
      BPlusTreeIndexNode index = bPlusTree.loadIndexNode(indexChannel, indexBuffer, pageAddress);
      compareIndexNodes(index, expectedindex);
      pageAddress++;
    }
  }

  private static void compareIndexNodes(
      BPlusTreeIndexNode index, BPlusTreeIndexNode expectedindex) {

    assertEquals(expectedindex.getNumOfKeys(), index.getNumOfKeys());
    for (int j = 0; j < index.getNumOfKeys(); j++) {

      if (expectedindex.getKeyAtIndex(j) != index.getKeyAtIndex(j)) {
        System.out.println(
            "Expected: " + expectedindex.getKeyAtIndex(j) + " Actual: " + index.getKeyAtIndex(j));
      }
      assertEquals(expectedindex.getKeyAtIndex(j), index.getKeyAtIndex(j));
    }

    // check each child
    for (int j = 0; j < index.getNumOfKeys() + 1; j++) {
      assertEquals(expectedindex.getChildAtIndex(j), index.getChildAtIndex(j));
    }
  }

  private static void compareLeafNodes(BPlusTreeLeafNode leaf, BPlusTreeLeafNode expectedLeaf) {
    assertEquals(expectedLeaf.getNumOfKeys(), leaf.getNumOfKeys());

    // check each key and entry
    for (int j = 0; j < leaf.getNumOfKeys(); j++) {
      assertEquals(expectedLeaf.getKeyAtIndex(j), leaf.getKeyAtIndex(j));

      // get the entry
      RecordEntry expectedEntry = expectedLeaf.getEntryAtIndex(j);
      RecordEntry entry = leaf.getEntryAtIndex(j);
      assertEquals(expectedEntry.getNumOfRecordIds(), entry.getNumOfRecordIds());

      // check each record id
      for (int k = 0; k < expectedEntry.getNumOfRecordIds(); k++) {

        RecordId expectedRecordId = expectedEntry.getRecordIdAtIndex(k);
        RecordId recordId = entry.getRecordIdAtIndex(k);

        assertEquals(expectedRecordId.pageNumber(), recordId.pageNumber());
        assertEquals(expectedRecordId.byteOffset(), recordId.byteOffset());
      }
    }
  }
}
