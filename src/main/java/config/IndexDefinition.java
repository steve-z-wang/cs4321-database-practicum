package config;

import java.util.List;
import net.sf.jsqlparser.schema.Column;

public class IndexDefinition {
  private final String relation;
  private final String attribute;
  private final boolean isClustered;
  private final int order;
  private final int attributeIndex;

  public IndexDefinition(String relation, String attribute, boolean isClustered, int order) {
    this.relation = relation;
    this.attribute = attribute;
    this.isClustered = isClustered;
    this.order = order;

    this.attributeIndex = findAttributeIndex();
  }

  public String getRelation() {
    return relation;
  }

  public String getAttribute() {
    return attribute;
  }

  public int getAttributeIndex() {
    return attributeIndex;
  }

  public boolean isClustered() {
    return isClustered;
  }

  public int getOrder() {
    return order;
  }

  @Override
  public String toString() {
    return String.format(
        "IndexEntry[relation=%s, attribute=%s, clustered=%s, order=%d]",
        relation, attribute, isClustered, order);
  }

  private int findAttributeIndex() {
    int attributeIndex = -1;
    List<Column> schema = DBCatalog.getInstance().getSchemaForTable(relation);
    for (int i = 0; i < schema.size(); i++) {
      if (schema.get(i).getColumnName().equals(attribute)) {
        attributeIndex = i;
        break;
      }
    }
    if (attributeIndex == -1) {
      throw new IllegalArgumentException(
          "Attribute " + attribute + " not found in schema for relation " + relation);
    }
    return attributeIndex;
  }
}
