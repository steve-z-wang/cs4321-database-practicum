package unitTest.physicaloperator.sort;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import config.PhysicalPlanConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;
import model.Tuple;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.OngoingStubbing;
import physicaloperator.base.PhysicalOperator;
import physicaloperator.sort.ExternalSortOperator;
import physicaloperator.sort.SortTupleComparator;
import testUtil.HelperMethods;

public class ExternalSortOperatorTest {

  private PhysicalOperator childOperator;
  private List<Column> childSchema;

  @BeforeEach
  void setUp() {

    childOperator = mock(PhysicalOperator.class);

    Table table = new Table("test");

    Column col1 = new Column(table, "col1");
    Column col2 = new Column(table, "col2");
    Column col3 = new Column(table, "col3");

    childSchema = Arrays.asList(col1, col2, col3);

    when(childOperator.getOutputSchema()).thenReturn(new ArrayList<>(childSchema));
  }

  @ParameterizedTest(name = "Operation with #{arguments} page buffer")
  @MethodSource("pageBufferConfigs")
  void testLargeDataset(int bufferPages) throws Exception {
    // set physcial config
    PhysicalPlanConfig config = PhysicalPlanConfig.getInstance();
    config.setSortConfig(PhysicalPlanConfig.SortMethod.EXTERNAL, bufferPages);

    // create comparator
    List<OrderByElement> orderByElements = new ArrayList<>();

    for (int i = 0; i < childSchema.size(); i++) {
      OrderByElement obe = new OrderByElement();
      obe.setExpression(childSchema.get(i));
      orderByElements.add(obe);
    }
    SortTupleComparator comparator = new SortTupleComparator(orderByElements, childSchema);

    // generate test data set
    Random random = new Random();
    ArrayList<Tuple> tuples = new ArrayList<>();

    for (int i = 0; i < 10000; i++) {
      int n1 = random.nextInt(10);
      int n2 = random.nextInt(100);
      int n3 = random.nextInt(1000);
      tuples.add(new Tuple(new ArrayList<>(Arrays.asList(n1, n2, n3))));
    }

    // set as mock data
    OngoingStubbing<Tuple> stubbing = when(childOperator.getNextTuple());
    for (Tuple tuple : tuples) {
      stubbing = stubbing.thenReturn(tuple);
    }
    stubbing.thenReturn(null);

    // expected tuples
    tuples.sort(comparator);

    // call the op
    PhysicalOperator op = new ExternalSortOperator(childOperator, orderByElements);

    List<Tuple> resultTuples = new ArrayList<>();
    Tuple tuple;
    while ((tuple = op.getNextTuple()) != null) {
      resultTuples.add(tuple);
    }

    // verify the result
    if (!HelperMethods.compareTupleListsExact(tuples, resultTuples)) {
      throw new AssertionError("Query returned different results (ignoring order)");
    }
  }

  private static IntStream pageBufferConfigs() {
    return IntStream.of(3, 5, 9, 10);
  }
}
