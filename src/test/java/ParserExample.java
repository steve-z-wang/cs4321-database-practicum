import common.DBCatalog;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;

/**
 * Example class for getting started with JSQLParser. Reads SQL statements from a file and prints
 * them to screen; then extracts SelectBody from each query and also prints it to screen.
 */
public class ParserExample {
  private final Logger logger = LogManager.getLogger();

  @Test
  public void parserExampleTest() throws IOException, JSQLParserException, URISyntaxException {
    ClassLoader classLoader = P1UnitTests.class.getClassLoader();

    URI resourceUri = Objects.requireNonNull(classLoader.getResource("samples/input")).toURI();

    Path resourcePath = Paths.get(resourceUri);

    DBCatalog.getInstance().setDataDirectory(resourcePath.resolve("db").toString());

    URI queriesUri =
        Objects.requireNonNull(classLoader.getResource("samples/input/queries.sql")).toURI();
    Path queriesFilePath = Paths.get(queriesUri);

    Statements statements = CCJSqlParserUtil.parseStatements(Files.readString(queriesFilePath));

    for (Statement statement : statements) {
      logger.info("Read statement: " + statement);

      Select select = (Select) statement;
      PlainSelect plainSelect = (PlainSelect) select;

      Table fromItem = (Table) plainSelect.getFromItem();
      logger.info("Table: " + fromItem);

      Expression where = plainSelect.getWhere();
      if (where != null) {
        logger.info("Where: " + where.toString());
      }

      List<SelectItem<?>> selectItems = plainSelect.getSelectItems();
      logger.info("Select: " + selectItems.toString());

      SelectItem<?> firstItem = selectItems.get(0);

      if (firstItem.getExpression() instanceof Column) {
        logger.info("First Select Item: " + firstItem.toString());
      } else {
        logger.info("First Select Item: " + firstItem.toString());
      }
    }
  }
}
