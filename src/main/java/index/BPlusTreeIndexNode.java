package index;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeIndexNode extends BPlusTreeNode {
  private List<Integer> keys;
  private List<Integer> children; // page addresses of child nodes

  public BPlusTreeIndexNode() {
    this.keys = new ArrayList<>();
    this.children = new ArrayList<>();
    this.isLeaf = false;
  }

  public void addKey(int key) {
    if (!keys.isEmpty() && key < keys.getLast()) {
      throw new IllegalArgumentException("Keys must be inserted in ascending order");
    }

    keys.add(key);
  }

  public int getKeyAtIndex(int index) {
    return keys.get(index);
  }

  public int getNumOfKeys() {
    return keys.size();
  }

  public int getChildAtIndex(int index) {
    return children.get(index);
  }

  public void addChild(int childAddress) {
    children.add(childAddress);
  }

  /**
   * Given the key, return the child node to go to
   *
   * @param targetKey the key to search for
   * @return the address of the child node
   */
  public int findChildByKey(int targetKey) {
    int i = 0;
    while (i < keys.size() && targetKey >= keys.get(i)) {
      i++;
    }
    return children.get(i);
  }

  @Override
  public void serialize(ByteBuffer buffer) throws IOException {

    // write flag
    buffer.putInt(1); // index node flag

    // write number of keys
    buffer.putInt(keys.size());

    // write keys
    for (int key : keys) {
      buffer.putInt(key);
    }

    // write address of children
    for (int childPageAddress : children) {
      buffer.putInt(childPageAddress);
    }
  }

  public static BPlusTreeIndexNode deserialize(ByteBuffer buffer) throws IOException {
    // read flag
    int indexFlag = buffer.getInt();
    if (indexFlag != 1) {
      throw new IOException("Not an index node");
    }

    // create a new index node
    BPlusTreeIndexNode indexNode = new BPlusTreeIndexNode();

    // read number of keys
    int numKeys = buffer.getInt();

    // read keys
    for (int i = 0; i < numKeys; i++) {
      indexNode.addKey(buffer.getInt());
    }

    // read children addresses
    for (int i = 0; i < numKeys + 1; i++) {
      indexNode.addChild(buffer.getInt());
    }

    return indexNode;
  }
}
