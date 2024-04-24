import java.sql.*;
import java.io.*;

/**
 * TSVDataLoader class loads data from TSV (Tab-Separated Values) files into an Oracle database.
 * It creates a new table for each year of data and populates it with the data from the corresponding TSV file.
 */
public class TSVDataLoader {

    /**
     * Main method that establishes a connection to the Oracle database and loads data from TSV files.
     * 
     * @param args Command line arguments where args[0] is the username and args[1] is the password for the database.
     */
    public static void main(String[] args) {
        // Database connection URL
        String jdbcUrl = "jdbc:oracle:thin:@aloe.cs.arizona.edu:1521:oracle";
        // Database credentials
        String username = args[0];
        String password = args[1];  
        // Array containing the years for which the data is available
        String[] fileYears = {"2010", "2014", "2018", "2022"};
        // Prefix for the TSV data files
        String filePrefix = "_cleaned.tsv";

        // Try-with-resources to automatically close the database connection
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            // Loop through each year and process the corresponding TSV file
            for (String year : fileYears) {
                // Construct the table name for the given year
                String tableName ="rishavk.emissions_data_"+ year;
                // Construct the file path for the TSV file
                String filePath = year + filePrefix;

                // Attempt to create a table for the given year
                try (Statement stmt = conn.createStatement()) {
                    // SQL statement for creating a table
                    String createTableSQL = String.format(
                        "CREATE TABLE %s (" +
                        "facility_id INT, " +
                        "facility_name VARCHAR2(255), " +
                        "city VARCHAR2(100), " +
                        "state VARCHAR2(50), " +
                        "zip_code VARCHAR2(20), " +
                        "address VARCHAR2(255), " +
                        "latitude DOUBLE PRECISION, " +
                        "longitude DOUBLE PRECISION, " +
                        "tr_direct_emissions DOUBLE PRECISION, " +
                        "co2_emissions_non_biogenic DOUBLE PRECISION, " +
                        "methane_emissions DOUBLE PRECISION, " +
                        "nitrous_oxide_emissions DOUBLE PRECISION, " +
                        "stationary_combustion DOUBLE PRECISION, " +
                        "electricity_generation DOUBLE PRECISION)", tableName);
                    // Execute the create table statement
                    stmt.execute(createTableSQL);
                } catch (SQLException e) {
                    // Catch and handle exceptions if table creation fails (e.g., table already exists)
                    System.out.println("Table " + tableName + " already exists or cannot be created.");
                    e.printStackTrace();
                }

                // Process the TSV file and load the data into the table
                try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                    // Skip the header line of the TSV file
                    br.readLine();

                    // Read the file line by line
                    String line;
                    while ((line = br.readLine()) != null) {
                        // Split each line into fields based on tabs
                        String[] values = line.split("\t");

                        // SQL statement for inserting data into the table
                        String sql = String.format("INSERT INTO %s (facility_id, facility_name, city, state, zip_code, address, latitude, longitude, tr_direct_emissions, co2_emissions_non_biogenic, methane_emissions, nitrous_oxide_emissions, stationary_combustion, electricity_generation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", tableName);

                        // Prepare the statement for batch execution
                        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                            // Set the facility_id from the first column of the TSV
                            pstmt.setInt(1, Integer.parseInt(values[0].trim()));
                            // Set text-based fields (facility_name through address)
                            for (int i = 1; i <= 5; i++) {
                                pstmt.setString(i + 1, values[i].trim());
                            }
                            // Set numeric fields (latitude through electricity_generation)
                            for (int i = 6; i < values.length; i++) {
                                // Handle empty numeric fields by setting them to null
                                if (!values[i].trim().isEmpty()) {
                                    pstmt.setDouble(i + 1, Double.parseDouble(values[i].trim()));
                                } else {
                                    pstmt.setNull(i + 1, java.sql.Types.DOUBLE);
                                }
                            }
                            // Execute the insert statement
                            pstmt.executeUpdate();
                        }
                    }
                } catch (IOException e) {
                    // Handle exceptions related to file reading
                    e.printStackTrace();
                } catch (NumberFormatException e) {
                    // Handle exceptions related to number format errors
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            // Handle exceptions related to database connection and SQL operations
            e.printStackTrace();
        }
    }
}

