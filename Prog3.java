import java.io.*;
import java.sql.*;
import java.util.InputMismatchException;
import java.util.Scanner;

/*
  This Java program, Prog3, serves as the main class for an application designed to interact with an Oracle database. It provides functionalities related to querying environmental data from various years. Here's an overview of the program structure and functionalities:

  1. The main method serves as the entry point for the application. It establishes a connection to the Oracle database using JDBC, reads command-line arguments for username and password, and then invokes the displayMenu method.
  2. The displayMenu method presents a menu to the user, allowing them to choose from different options such as querying facility change percentage, calculating distances between facilities, retrieving top emission facilities, executing custom queries, and exiting the program.
  3. Methods such as queryFacilityChangePercentage, queryDistanceBetweenFacilities, queryTopEmissionFacilities, and customQuery implement specific functionalities corresponding to the menu options. They interact with the database to retrieve and process relevant data.
  4. The program utilizes JDBC to establish a connection to the Oracle database, execute SQL queries, and process the retrieved data.
  5. Error handling is implemented to handle potential exceptions such as ClassNotFoundException, SQLException, and InputMismatchException.

  Overall, Prog3 provides a versatile interface for querying environmental data stored in an Oracle database, empowering users to retrieve valuable insights and perform custom analyses.
  Author: Rishav Kumar
  Class: 460
  Asignment: Program#3
*/
// Main class for Prog3
public class Prog3 {

  // Entry point for the Prog3 application
  public static void main(String[] args) {
    // Oracle DB connection string
    final String oracleURL = "jdbc:oracle:thin:@aloe.cs.arizona.edu:1521:oracle";
    // Variables for storing the username and password
    String username = "", password = "";

    // Retrieve the username and password from command line arguments, if provided
    if (args.length == 2) {
      username = args[0];
      password = args[1];
    }

    // Attempt to load the Oracle JDBC driver
    try {
      Class.forName("oracle.jdbc.OracleDriver");
    } catch (ClassNotFoundException e) {
      System.err.println("*** ClassNotFoundException: Error loading Oracle JDBC driver.");
      System.exit(-1);
    }

    // Establish a connection to the database and display the menu
    try (Connection dbconn = DriverManager.getConnection(oracleURL, username, password)) {
      displayMenu(dbconn);
    } catch (SQLException e) {
      System.err.println("*** SQLException: Could not open JDBC connection.");
      System.exit(-1);
    }
  }

  // Displays a menu to the user and processes the user's choice
  private static void displayMenu(Connection dbconn) {
    Scanner scanner = new Scanner(System.in);
    while (true) {
      // Display menu options
      System.out.println("\nSelect an option:");
      System.out.println("1. Facility quantity change by percentage between years");
      System.out.println("2. Calculate distance between two facilities");
      System.out.println("3. Top ten total reported direct emission facilities per year");
      System.out.println("4. Custom query");
      System.out.println("5. Exit");
      System.out.print("Choice: ");
      
      // Read the user's choice
      int choice = scanner.nextInt();

      // Execute the appropriate action based on the user's choice
      switch (choice) {
        case 1:
          queryFacilityChangePercentage(dbconn);
          break;
        case 2:
          queryDistanceBetweenFacilities(dbconn, scanner);
          break;
        case 3:
          queryTopEmissionFacilities(dbconn);
          break;
        case 4:
          customQuery(dbconn, scanner);
          break;
        case 5:
          System.out.println("Exiting program.");
          return;
        default:
          System.out.println("Invalid choice. Please select again.");
      }
    }
  }

  // Method to query the percentage change in facility count between pairs of years
  private static void queryFacilityChangePercentage(Connection dbconn) {
    // Array of years to compare
    String[] years = { "2010", "2014", "2018", "2022" };
    // Loop through the year pairs
    for (int i = 0; i < years.length - 1; i++) {
      try {
        // Construct the SQL queries for each year pair
        String tableFirstYear = "rishavk.emissions_data_" + years[i];
        String tableSecondYear = "rishavk.emissions_data_" + years[i + 1];
        // Query to get the count of facilities for the first year
        String queryFirstYear = String.format("SELECT COUNT(*) AS count FROM %s", tableFirstYear);
        Statement stmtFirstYear = dbconn.createStatement();
        ResultSet rsFirstYear = stmtFirstYear.executeQuery(queryFirstYear);
        rsFirstYear.next();
        int countFirstYear = rsFirstYear.getInt("count");

        rsFirstYear.close();
        stmtFirstYear.close();

        // Query to get the count of facilities for the second year
        String querySecondYear = String.format("SELECT COUNT(*) AS count FROM %s", tableSecondYear);
        Statement stmtSecondYear = dbconn.createStatement();
        ResultSet rsSecondYear = stmtSecondYear.executeQuery(querySecondYear);
        rsSecondYear.next();
        int countSecondYear = rsSecondYear.getInt("count");
        rsSecondYear.close();
        stmtSecondYear.close();

        // Calculate and print the percentage change in facility count
        double percentageChange = ((double) (countSecondYear - countFirstYear) / countFirstYear) * 100;
        System.out.printf("From %s to %s, the quantity of facilities changed by %.4f%%\n", years[i], years[i + 1],
            percentageChange);
      } catch (SQLException e) {
        System.err.println("SQLException: " + e.getMessage());
      }
    }
  }

  // Method to calculate the distance between two facilities
  private static void queryDistanceBetweenFacilities(Connection dbconn, Scanner scanner) {
    try {
      // Prompt the user for the year and facility IDs
      System.out.print("Enter the year (2010, 2014, 2018, 2022): ");
      String year = scanner.next();
      System.out.print("Enter the first facility ID: ");
      int facilityId1 = scanner.nextInt();
      System.out.print("Enter the second facility ID: ");
      int facilityId2 = scanner.nextInt();

      // Retrieve the coordinates for both facilities and calculate the distance
      String tableName = "rishavk.emissions_data_" + year;

      double[] facility1Coords = getFacilityCoordinates(dbconn, tableName, facilityId1);
      double[] facility2Coords = getFacilityCoordinates(dbconn, tableName, facilityId2);

      if (facility1Coords != null && facility2Coords != null) {
        double distance = calculateGreatCircleDistance(facility1Coords[0], facility1Coords[1], facility2Coords[0],
            facility2Coords[1]);
        System.out.printf("The distance between facility %d and facility %d is %.4f nautical miles.\n", facilityId1,
            facilityId2, distance);
      } else {
        System.out.println("One or both facility IDs were not found.");
      }
    } catch (SQLException e) {
      System.err.println("SQLException: " + e.getMessage());
    }
  }

  // Helper method to retrieve latitude and longitude coordinates for a given facility ID
  private static double[] getFacilityCoordinates(Connection dbconn, String tableName, int facilityId)
      throws SQLException {
    String query = String.format("SELECT latitude, longitude FROM %s WHERE facility_id = ?", tableName);
    try (PreparedStatement pstmt = dbconn.prepareStatement(query)) {
      pstmt.setInt(1, facilityId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          double latitude = rs.getDouble("latitude");
          double longitude = rs.getDouble("longitude");
          return new double[] { latitude, longitude };
        }
      }
    }
    return null; // Return null if the facility ID was not found
  }

  // Method to calculate the great circle distance between two points on the earth
  private static double calculateGreatCircleDistance(double lat1, double lon1, double lat2, double lon2) {
    // Convert latitude and longitude from degrees to radians
    lat1 = Math.toRadians(lat1);
    lon1 = Math.toRadians(lon1);
    lat2 = Math.toRadians(lat2);
    lon2 = Math.toRadians(lon2);

    // Haversine formula to calculate the distance
    double dLat = lat2 - lat1;
    double dLon = lon2 - lon1;
    double a = Math.pow(Math.sin(dLat / 2), 2) +
        Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dLon / 2), 2);
    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    // Earth radius in nautical miles
    double earthRadius = 3440.065;
    // Calculate and return the distance
    return earthRadius * c;
  }

  // Method to query the top ten emission facilities for each year
  private static void queryTopEmissionFacilities(Connection dbconn) {
    // Iterate through each year and execute a query to find the top facilities by emissions
    String[] years = { "2010", "2014", "2018", "2022" };
    for (String year : years) {
      // Print the header for the results
      System.out.println("Year: " + year);
      System.out.printf("%-50s %-20s %13s\n", "Facility Name", "State", "Emissions");
      // Separator line for the header
      System.out.println("----------------------------------------------------------------------------------------");
      // Construct the query for the current year
      String tableName = "rishavk.EMISSIONS_DATA_" + year;

      String query = String.format(
          "SELECT FACILITY_NAME, STATE, SUM(TOTAL_REPORTED_DIRECT_EMISSIONS) AS total_emissions " +
              "FROM %s GROUP BY FACILITY_NAME, STATE ORDER BY total_emissions DESC FETCH FIRST 10 ROWS ONLY",
          tableName);

      // Execute the query and print the results
      try (Statement stmt = dbconn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
        while (rs.next()) {
          String facilityName = rs.getString("FACILITY_NAME");
          String state = rs.getString("STATE");
          double totalEmissions = rs.getDouble("total_emissions");
          System.out.printf("%-50s %-20s %13.7E\n", facilityName, state, totalEmissions);
        }
        // Print a blank line after each year's results
        System.out.println();
      } catch (SQLException e) {
        System.err.println("SQLException: " + e.getMessage());
      }
    }
  }

  // Custom query method designed by the user's specifications
  private static void customQuery(Connection dbconn, Scanner scanner) {
    // Prompt the user for state and range of emissions and execute a query across all tables
    try {
      // User inputs for the state and emission range
      System.out.print("Enter the state abbreviation (e.g., CA): ");
      String state = scanner.next();
      System.out.print("Enter the minimum total reported direct emissions: ");
      double minEmissions = scanner.nextDouble();
      System.out.print("Enter the maximum total reported direct emissions: ");
      double maxEmissions = scanner.nextDouble();

      // Years to iterate over
      String[] years = { "2010", "2014", "2018", "2022" };
      // Query builder for constructing the SQL query
      StringBuilder queryBuilder = new StringBuilder();

      // Loop through each year and append the query to the builder
      for (int i = 0; i < years.length; i++) {
        queryBuilder.append(
            String.format(
                "SELECT '%s' AS year, FACILITY_NAME, STATE, TR_DIRECT_EMISSIONS " +
                    "FROM rishavk.emissions_data_%s " +
                    "WHERE STATE = '%s' " +
                    "AND TR_DIRECT_EMISSIONS BETWEEN %.2f AND %.2f",
                years[i], years[i], state, minEmissions, maxEmissions));
        if (i < years.length - 1) {
          queryBuilder.append(" UNION ALL ");
        }
      }

      // Execute the constructed query and print results
      try (Statement stmt = dbconn.createStatement(); ResultSet rs = stmt.executeQuery(queryBuilder.toString())) {
        System.out.println("Year | Facility Name                                | State | Emissions");
        // Separator line for the table header
        System.out.println("--------------------------------------------------------------------------------");
        // Iterate through the result set and print each record
        while (rs.next()) {
          String year = rs.getString("year");
          String facilityName = rs.getString("FACILITY_NAME");
          String facilityState = rs.getString("STATE");
          double totalEmissions = rs.getDouble("TR_DIRECT_EMISSIONS");
          System.out.printf("%4s | %-44s | %5s | %.2f\n", year, facilityName, facilityState, totalEmissions);
        }
      }

    } catch (SQLException e) {
      System.err.println("SQLException: " + e.getMessage());
    } catch (InputMismatchException e) {
      System.err.println("InputMismatchException: Please enter the correct data types.");
    }
  }

}

