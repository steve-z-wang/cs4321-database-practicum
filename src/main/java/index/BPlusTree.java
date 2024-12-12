package index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import utils.DBConstants;
import utils.ParallelListSorter;

public class BPlusTree {

  private int rootAddress;
  private int numLeafNodes;
  private int order;

  public int getRootAddress() {
    return rootAddress;
  }

  public int getNumLeafNodes() {
    return numLeafNodes;
  }

  public int getOrder() {
    return order;
  }

  public BPlusTreeLeafNode getFirstLeafNode(FileChannel indexChannel, ByteBuffer buffer)
      throws IOException {
    return loadLeafNode(indexChannel, buffer, 1);
  }

  public BPlusTreeLeafNode findLeafNodeByKey(
      FileChannel indexChannel, ByteBuffer buffer, int targetKey) throws IOException {

    // load the root node
    BPlusTreeIndexNode node = loadIndexNode(indexChannel, buffer, rootAddress);

    while (true) {

      // find next child
      int childAddress = node.findChildByKey(targetKey);

      // if the child is a leaf node, return the leaf node
      if (isLeafNode(childAddress)) {
        return loadLeafNode(indexChannel, buffer, childAddress);
      }

      node = loadIndexNode(indexChannel, buffer, childAddress);
    }
  }

  public BPlusTreeLeafNode getNextLeafNode(
      FileChannel indexChannel, ByteBuffer buffer, BPlusTreeLeafNode leafNode) throws IOException {

    int nextAddress = leafNode.getAddress() + 1;

    // if the next address is not a leaf node
    if (!isLeafNode(nextAddress)) {
      return null;
    }

    return loadLeafNode(indexChannel, buffer, leafNode.getAddress() + 1);
  }

  /** Serialize a B+ tree to disk */
  public static void buildAndSerializeBPlusTree(
      FileChannel channel,
      ByteBuffer buffer,
      List<Integer> keys,
      List<RecordId> recordIds,
      int order)
      throws IOException {
    if (keys.isEmpty()) {
      return;
    }

    // sort the key and recordId lists and build RecordEntries
    ParallelListSorter.sortParallelLists(keys, recordIds);
    List<RecordEntry> entries = buildRecordEntries(keys, recordIds);

    List<BPlusTreeNode> treeNodes = new ArrayList<>();
    treeNodes.add(new BPlusTreeLeafNode());

    // build leaf nodes level
    int currentLevelStart = 1;
    int leafNodeCreated = buildLeafNodeLevel(treeNodes, currentLevelStart, entries, order);

    // build index nodes levels
    int nodeCreated = leafNodeCreated;
    do {
      int prevLevelStart = currentLevelStart;
      currentLevelStart = prevLevelStart + nodeCreated;
      nodeCreated = buildIndexNodeLevel(treeNodes, prevLevelStart, currentLevelStart, order);
    } while (nodeCreated != 1);

    // Write Tree to File
    writeBPlusTree(channel, order, treeNodes, buffer, leafNodeCreated);
  }

  /**
   * Deserialize a B+ tree index node from a
   *
   * @return the root node of the B+ tree index
   */
  public static BPlusTree deserialize(FileChannel indexFilechannel, ByteBuffer buffer)
      throws IOException {

    BPlusTree tree = new BPlusTree();

    // read the first page of the index file
    loadPage(indexFilechannel, buffer, 0);
    tree.rootAddress = buffer.getInt(0);
    tree.numLeafNodes = buffer.getInt(4);
    tree.order = buffer.getInt(8);

    return tree;
  }

  /** Helper methods for traversing a B+ tree */
  private boolean isLeafNode(int nodeAddress) {
    return nodeAddress <= numLeafNodes;
  }

  /** Helper method for building and serializing a B+ tree */
  private static void writeBPlusTree(
      FileChannel channel,
      int order,
      List<BPlusTreeNode> treeNodes,
      ByteBuffer buffer,
      int leafNodeCreated)
      throws IOException {
    // write the header
    int rootAddress = treeNodes.size() - 1;
    writeBPlusTreeHeader(channel, buffer, rootAddress, leafNodeCreated, order);

    // write the nodes
    for (int nodeAddress = 1; nodeAddress < treeNodes.size(); nodeAddress++) {
      writeBPlusTreeNode(channel, buffer, treeNodes.get(nodeAddress), nodeAddress);
    }
  }

  private static void writeBPlusTreeHeader(
      FileChannel channel, ByteBuffer buffer, int rootAddress, int numLeafNodes, int order)
      throws IOException {
    channel.position(0);
    buffer.clear();
    buffer.putInt(rootAddress);
    buffer.putInt(numLeafNodes);
    buffer.putInt(order);
    buffer.flip();

    int bytesWritten = channel.write(buffer);
  }

  private static void writeBPlusTreeNode(
      FileChannel channel, ByteBuffer buffer, BPlusTreeNode node, int curNodeAddress)
      throws IOException {
    channel.position((long) curNodeAddress * DBConstants.INDEX_PAGE_SIZE);

    buffer.clear();
    node.serialize(buffer);
    buffer.flip();

    int bytesWritten = channel.write(buffer);
  }

  private static List<RecordEntry> buildRecordEntries(
      List<Integer> keys, List<RecordId> recordIds) {
    // change the keys and recordIds into DataEntries
    List<RecordEntry> entries = new ArrayList<>();

    // create the first entry
    RecordEntry entry = new RecordEntry(keys.getFirst());

    // loop through the keys and recordIds
    for (int i = 0; i < keys.size(); i++) {
      int currentKey = keys.get(i);
      RecordId currentRecordId = recordIds.get(i);

      // if there is a new key
      if (currentKey != entry.getKey()) {
        // add the entry to the list & create a new entry
        entries.add(entry);
        entry = new RecordEntry(currentKey);
      }

      // add the recordId to the entry
      entry.addRecordId(currentRecordId);
    }
    entries.add(entry);
    return entries;
  }

  private static void buildLeafNode(
      List<BPlusTreeNode> treeNodes,
      List<RecordEntry> entries,
      int startIndex,
      int endIndex,
      int currentNodeAddress) {
    BPlusTreeLeafNode leafNode = new BPlusTreeLeafNode();

    leafNode.setAddress(currentNodeAddress);

    for (int i = startIndex; i < endIndex; i++) {
      leafNode.addEntry(entries.get(i));
    }

    // add the leaf node to the list of tree nodes
    treeNodes.add(leafNode);
  }

  private static int buildLeafNodeLevel(
      List<BPlusTreeNode> treeNodes, int nextNodeAddress, List<RecordEntry> entries, int d) {

    int currentEntryIndex = 0;
    int nodesCreated = 0;
    while (currentEntryIndex < entries.size()) {

      int k = entries.size() - currentEntryIndex;

      // if there are less than 2d entries
      if (k > 2 * d && k < 3 * d) {
        int midPoint = currentEntryIndex + k / 2;
        buildLeafNode(treeNodes, entries, currentEntryIndex, midPoint, nextNodeAddress++);
        nodesCreated++;
        currentEntryIndex = midPoint;
      }

      // calculate the end index
      int endIndex = Math.min(currentEntryIndex + 2 * d, entries.size());
      buildLeafNode(treeNodes, entries, currentEntryIndex, endIndex, nextNodeAddress++);
      nodesCreated++;
      currentEntryIndex = endIndex;
    }

    return nodesCreated;
  }

  /**
   * Helper method to find the left most key of a node
   *
   * @param node the node to find the left most key
   * @return the left most key
   */
  private static int findLeftMostKey(List<BPlusTreeNode> treeNodes, BPlusTreeNode node) {
    if (node.isLeaf()) {
      return node.getKeyAtIndex(0);
    }

    // if the node is an index node
    BPlusTreeIndexNode indexNode = (BPlusTreeIndexNode) node;

    // get the left most child
    int leftMostChildAddress = indexNode.getChildAtIndex(0);
    BPlusTreeNode childNode = treeNodes.get(leftMostChildAddress);

    // recursively find the left most leaf node
    return findLeftMostKey(treeNodes, childNode);
  }

  private static void buildIndexNode(
      List<BPlusTreeNode> treeNodes, int startIndex, int endIndex, int nextNodeAddress) {
    BPlusTreeIndexNode indexNode = new BPlusTreeIndexNode();
    indexNode.setAddress(nextNodeAddress);
    int readIndex = startIndex;

    BPlusTreeNode childNode = treeNodes.get(readIndex++);
    indexNode.addChild(childNode.getAddress());

    while (readIndex < endIndex) {
      childNode = treeNodes.get(readIndex++);

      int key = findLeftMostKey(treeNodes, childNode);
      indexNode.addKey(key);
      indexNode.addChild(childNode.getAddress());
    }

    treeNodes.add(indexNode);
  }

  private static int buildIndexNodeLevel(
      List<BPlusTreeNode> treeNodes, int lastLevelStartIndex, int lastLevelEndIndex, int d) {

    int readIndex = lastLevelStartIndex;
    int nextNodeAddress = lastLevelEndIndex;
    int nodesCreated = 0;
    while (readIndex < lastLevelEndIndex) {

      int m = lastLevelEndIndex - readIndex;

      // if there are less than 2d entries
      if (m > 2 * d + 1 && m < 3 * d + 2) {

        int midPoint = readIndex + m / 2;
        buildIndexNode(treeNodes, readIndex, midPoint, nextNodeAddress++);
        nodesCreated++;
        readIndex = midPoint;
      }

      // calculate the end index
      int endIndex = Math.min(readIndex + 2 * d + 1, lastLevelEndIndex);
      buildIndexNode(treeNodes, readIndex, endIndex, nextNodeAddress++);
      nodesCreated++;
      readIndex = endIndex;
    }

    return nodesCreated;
  }

  /** Helper methods for deserializing a B+ tree */
  public BPlusTreeIndexNode loadIndexNode(FileChannel channel, ByteBuffer buffer, int nodeAddress)
      throws IOException {
    loadPage(channel, buffer, nodeAddress);
    return BPlusTreeIndexNode.deserialize(buffer);
  }

  public BPlusTreeLeafNode loadLeafNode(FileChannel channel, ByteBuffer buffer, int nodeAddress)
      throws IOException {
    loadPage(channel, buffer, nodeAddress);
    return BPlusTreeLeafNode.deserialize(buffer);
  }

  private static void loadPage(FileChannel indexChannel, ByteBuffer buffer, int nodeAddress)
      throws IOException {
    buffer.clear();
    int bytesRead = indexChannel.read(buffer, (long) nodeAddress * DBConstants.INDEX_PAGE_SIZE);
    if (bytesRead != DBConstants.INDEX_PAGE_SIZE) {
      throw new IOException("Failed to read complete page at nodeAddress: " + nodeAddress);
    }
    buffer.flip();
  }
}
