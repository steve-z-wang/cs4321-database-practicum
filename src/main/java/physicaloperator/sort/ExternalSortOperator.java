package physicaloperator.sort;

import static config.PhysicalPlanConfig.INT_SIZE;
import static config.PhysicalPlanConfig.PAGE_SIZE;

import config.PhysicalPlanConfig;
import io.cache.CacheFileManager;
import io.reader.BinaryTupleReader;
import io.reader.TupleReader;
import io.writer.BinaryTupleWriter;
import io.writer.TupleWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import model.Tuple;
import net.sf.jsqlparser.statement.select.OrderByElement;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import physicaloperator.base.PhysicalOperator;

public class ExternalSortOperator extends PhysicalOperator {
  private static final Logger logger = LogManager.getLogger(ExternalSortOperator.class);

  private final PhysicalOperator childOperator;
  private final int pagesPerBlock;
  private final int initPassBufferSize;
  private final SortTupleComparator tupleComparator;

  private CacheFileManager cacheFileManager;
  private BinaryTupleReader resultReader = null;

  public ExternalSortOperator(PhysicalOperator operator, List<OrderByElement> orderByElements) {
    super(operator.getOutputSchema());

    PhysicalPlanConfig config = PhysicalPlanConfig.getInstance();
    if (config.getSortMethod() != PhysicalPlanConfig.SortMethod.EXTERNAL) {
      throw new RuntimeException("Calling external sort operator but expect in memeory sort");
    }

    this.childOperator = operator;

    // the total memory that we can use at a time
    this.pagesPerBlock = config.getSortBufferPages();
    this.initPassBufferSize = (this.pagesPerBlock-1) * PAGE_SIZE / (this.outputSchema.size() * INT_SIZE);

    this.tupleComparator = new SortTupleComparator(orderByElements, this.outputSchema);

    try {
      this.cacheFileManager = new CacheFileManager();
    } catch (IOException e) {
      logger.error("Error creating ExternalSortOperator: ", e);
    }

    // create the reader for the result file
    String resultFile = sort();
    try {
      FileChannel channel = cacheFileManager.getReadChannel(resultFile);
      resultReader = new BinaryTupleReader(channel);
    } catch (IOException e) {
      logger.error("Error creating result file reader:", e);
    }
  }

  public String sort() {
    List<String> runFiles = createInitialRuns();

    int passCount = 1;
    while (runFiles.size() > 1) {
      runFiles = createMergeRuns(passCount++, runFiles);
    }

    return runFiles.getFirst();
  }

  @Override
  public void reset() {
    resultReader.reset();
  }

  @Override
  public void reset(int index) {
    resultReader.reset(index);
  }

  @Override
  public Tuple getNextTuple() {
    return resultReader.getNextTuple();
  }

  /**
   * Read from child operator and store all tuples to cache files
   *
   * @return tempFiles A list of created file
   */
  private List<String> createInitialRuns() {
    logger.debug("Creating initial runs ");

    List<String> runFiles = new ArrayList<>();
    // use the whole block as we only use one buffer at a time for the first pass
    ArrayList<Tuple> buffer = new ArrayList<>(initPassBufferSize);

    int runCount = 0;
    Tuple tuple = this.childOperator.getNextTuple();
    while (tuple != null) {
      buffer.clear();

      // fill the buffer ()
      while (buffer.size() < initPassBufferSize && tuple != null) {
        buffer.add(tuple);
        tuple = this.childOperator.getNextTuple();
      }

      // sort the buffer
      buffer.sort(this.tupleComparator);

      // store the buffer
      String runFileName = String.format("run_0_%d.tmp", runCount);
      try (FileChannel channel = cacheFileManager.getWriteChannel(runFileName);
          TupleWriter writer = new BinaryTupleWriter(channel, this.outputSchema.size())) {
        writer.writeTuples(buffer);
      } catch (IOException e) {
        logger.error("Error creating initial runs: ", e);
      }
      runFiles.add(runFileName);
      runCount++;
    }
    return runFiles;
  }

  private List<String> createMergeRuns(int passCount, List<String> runFiles) {
    logger.debug("Creating runs for the {}-th merge pass",  passCount );

    List<String> newRunFiles = new ArrayList<>();
    // we have B page, B-1 pages are for the readers
    // leaving one page for the buffer

    int runCount = 0;
    int nextFileIndex = 0;
    while (nextFileIndex < runFiles.size()) {

      // determine the files to process
      List<String> filesToProcess = new ArrayList<>();
      for (int i = 0; i < pagesPerBlock - 1 && nextFileIndex < runFiles.size(); i++) {
        filesToProcess.add(runFiles.get(nextFileIndex++));
      }

      // create the heap
      PriorityQueue<HeapNode> minHeap = initialMergeHeap(filesToProcess);

      // create the new run file
      String runFileName = String.format("run_%d_%d.tmp", passCount, runCount);
      writeMergeRun(runFileName, minHeap);

      // delete the old run file
      logger.debug("Deleting previous run {}", filesToProcess);
      try {
        for (String file: filesToProcess) {
          cacheFileManager.deleteFile(file);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      newRunFiles.add(runFileName);
      runCount++;
    }

    return newRunFiles;
  }

  private void writeMergeRun(String runFileName, PriorityQueue<HeapNode> minHeap) {
    logger.debug("Creating run {}", runFileName);
    try (FileChannel channel = cacheFileManager.getWriteChannel(runFileName);
         TupleWriter writer = new BinaryTupleWriter(channel, this.outputSchema.size())) {

      // writing all tuple from readers to the file
      while (!minHeap.isEmpty()) {
        HeapNode node = minHeap.poll();
        writer.writeTuple(node.tuple());

        // if the reader is not empty, add the reader back to the heap
        Tuple tuple = node.reader().getNextTuple();
        if (tuple != null) {
          minHeap.add(new HeapNode(tuple, node.reader()));
        } else {
          node.reader().close();
        }
      }
    } catch (IOException e) {
      logger.error("Fail creating cache file", e);
    }
  }

  private PriorityQueue<HeapNode> initialMergeHeap(List<String> filesToProcess) {
    logger.debug("Initialize heap for {}", filesToProcess);
    PriorityQueue<HeapNode> minHeap = new PriorityQueue<>(
                    (node1, node2) -> this.tupleComparator.compare(node1.tuple(), node2.tuple()));
    try {
      for (String file: filesToProcess) {
        FileChannel channel = cacheFileManager.getReadChannel(file);
        TupleReader reader = new BinaryTupleReader(channel);
        Tuple firstTuple = reader.getNextTuple();
        if (firstTuple != null) {
          minHeap.offer(new HeapNode(firstTuple, reader));
        }
      }
    } catch (IOException e) {
      logger.error("Error creating cache file reader: ", e);
    }
    return minHeap;
  }

}

/**
 * @param reader Your object that contains the list of values
 */
record HeapNode(Tuple tuple, TupleReader reader) {}
