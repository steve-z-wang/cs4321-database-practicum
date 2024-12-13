package index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BPlusTreeLeafNode extends BPlusTreeNode {
  private List<RecordEntry> entries;

  public BPlusTreeLeafNode() {
    super();
    this.entries = new ArrayList<>();
    this.isLeaf = true;
  }

  public void addEntry(RecordEntry entry) {
    entries.add(entry);
  }

  /**
   * This find the first key that is greater than or equal to the target key TODO: upgrade to binary
   * search
   */
  public int findEntryIndexByKey(int targetKey) {
    int i = 0;
    while (i < entries.size() && targetKey > entries.get(i).getKey()) {
      i++;
    }

    return i;
  }

  @Override
  public int getKeyAtIndex(int index) {
    return entries.get(index).getKey();
  }

  public RecordEntry getEntryAtIndex(int index) {
    return entries.get(index);
  }

  @Override
  public int getNumOfKeys() {
    return entries.size();
  }

  @Override
  public void serialize(ByteBuffer buffer) throws IOException {

    // write flag
    buffer.putInt(0);

    // write number of keys
    buffer.putInt(entries.size());

    // entries
    for (RecordEntry entry : entries) {
      entry.serialize(buffer);
    }
  }

  public static BPlusTreeLeafNode deserialize(ByteBuffer buffer) throws IOException {
    // read flag
    int leafFlag = buffer.getInt();
    if (leafFlag != 0) {
      throw new IOException("Not a leaf node");
    }

    // create a new leaf node
    BPlusTreeLeafNode leafNode = new BPlusTreeLeafNode();

    // read number of keys
    int numKeys = buffer.getInt();

    for (int i = 0; i < numKeys; i++) {
      RecordEntry entry = RecordEntry.deserialize(buffer);
      leafNode.addEntry(entry);
    }

    return leafNode;
  }
}
