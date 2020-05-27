import java.io.*;
import java.sql.*;
 
public class SimpleCsv2DbInserter {
 
    public static void main(String[] args) throws ClassNotFoundException {
//        String jdbcURL = "jdbc:mysql://localhost:3306/mangal";
//        String username = "admin";
//        String password = "password";
// 
        String csvFilePath = "/home/mangal/Downloads/realestate1.csv";
 
        int batchSize = 20;
 
        Connection connection = null;
 
        try {
        	Class.forName("com.mysql.jdbc.Driver");

			 connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mangal", "admin", "password");
 
            //connection = DriverManager.getConnection(jdbcURL, username, password);
            connection.setAutoCommit(false);
 
            String sql = "INSERT INTO realestate (street, city, zip, state, beds, baths, sq__ft, type, sale_date, price, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )";
            PreparedStatement statement = connection.prepareStatement(sql);
 
            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;
 
            int count = 0;
 
            lineReader.readLine(); // skip header line
 
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");
               /* String courseName = data[0];
                String studentName = data[1];
                String timestamp = data[2];
                String rating = data[3];
                String comment = data.length == 5 ? data[4] : "";
               */ 
                
                String street = data[0];
                String city = data[1];
                String zip = data[2];
                String state = data[3];
                String beds = data[4];
                String baths = data[5];
                String sq__ft = data[6];
                String type = data[7];
                String sale_date = data[8];
                String price = data[9];
                String latitude  = data[10];
                String longitude = data[11];
                
 
                statement.setString(1, street);
                statement.setString(2, city);
                statement.setString(3, zip);
                statement.setString(4, state);
                statement.setInt(5, Integer.parseInt(beds));
                statement.setInt(6, Integer.parseInt(baths));
                statement.setInt(7, Integer.parseInt(sq__ft));
                statement.setString(8, type);
                statement.setString(9, sale_date);
                statement.setInt(10, Integer.parseInt(price));
                statement.setString(11, latitude);
                statement.setString(12, longitude);
 
                
                statement.addBatch();
                System.out.print("\nStart");
 
                if (count % batchSize == 0) {
                    statement.executeBatch();
                }
            }
 
            lineReader.close();
 
            // execute the remaining queries
            statement.executeBatch();
 
            connection.commit();
            connection.close();
            System.out.print("\nEnd");
 
        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();
 
            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
 
    }
}