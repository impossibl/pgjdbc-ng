import java.sql.*;

public class Verify {

  public static void main(String[] args) throws SQLException {
    DatabaseMetaData metaData = DriverManager.getConnection(args[0]).getMetaData();
    String product = metaData.getDatabaseProductName() + " (" + metaData.getDatabaseProductVersion() + ")";
    System.out.println("Success: " + product);
  }

}
