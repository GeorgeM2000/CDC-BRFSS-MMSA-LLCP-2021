import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;

public class SingleHashJoin {

    // This class will be used for the hash table buckets
    static class BucketKey {
        int micHd;
        String month;

        public BucketKey(int micHd, String month) {
            this.micHd = micHd;
            this.month = month;
        }

        // Override equals and hashCode methods to ensure correct behavior when using BucketKey as a key in a HashMap
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BucketKey bucketKey = (BucketKey) o;
            return micHd == bucketKey.micHd && Objects.equals(month, bucketKey.month);
        }

        @Override
        public int hashCode() {
            return Objects.hash(micHd, month);
        }

        public int getMicHd() {
            return micHd;
        }

        public String getMonth() {
            return month;
        }
    }

    public static void main(String[] args) {
        String mmsa = "CDC_BRFSS_Datasets/MMSA_2021_Undersampled_VerB.csv";
        String llcp = "CDC_BRFSS_Datasets/LLCP_2021_Undersampled_VerB.csv";

        String mmsaUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_MMSA";
        String llcpUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_LLCP";
        String dbUsername = "root";
        String dbPassword = "---";


        /*
        Simple Hash-Join Algorithm
         */

        // CREATE the hash buckets based on the combinations of _MICHD and Timestamp


        long startTime = System.currentTimeMillis();

        // Possible values for the join attributes
        int[] micHdValues = {1, 2};
        String[] months = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};

        // Define a hash table to store rows based on the combination of values
        Map<BucketKey, List<Map<String, Object>>> hashTable = new HashMap<>();

        // The matching rows
        int matchingCount = 0;

        // Populate the hash table
        for (int micHd : micHdValues) {
            for (String month : months) {
                BucketKey bucketKey = new BucketKey(micHd, month);
                hashTable.put(bucketKey, new ArrayList<>());
            }
        }

        // Print the buckets
        System.out.println("Number of Buckets: " + hashTable.size());
        for (Map.Entry<BucketKey, List<Map<String, Object>>> entry : hashTable.entrySet()) {
            System.out.println("Bucket: " + entry.getKey().micHd + "_" + entry.getKey().month);
        }



        // BUILDING PHASE

        // Establish a connection to the database
        try (Connection connection = DriverManager.getConnection(mmsaUrl, dbUsername, dbPassword)) {
            // Retrieve rows from the "MMSA" table
            String sql = "SELECT * FROM mmsa";
            try (Statement statement = connection.createStatement();
                 ResultSet resultSet = statement.executeQuery(sql)) {

                // Populate the hash table with rows from the "MMSA" table
                while (resultSet.next()) {
                    int micHd = resultSet.getInt("_MICHD"); // Get the _MICHD value
                    String month = resultSet.getString("Timestamp").substring(5, 7); // Get the month value
                    Map<String, Object> row = new LinkedHashMap<>(); // row will store the column values for the current row

                    // Store all columns of the row
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = resultSet.getObject(i);
                        row.put(columnName, columnValue);
                    }

                    // Create a BucketKey object for the current row
                    BucketKey bucketKey = new BucketKey(micHd, month);
                    // Get the list of rows for the corresponding bucket
                    List<Map<String, Object>> rows = hashTable.computeIfAbsent(bucketKey, k -> new ArrayList<>());

                    // Add the current row to the list of rows for the bucket
                    rows.add(row);
                }
            }

            // Print the contents of the hash table
            /*
            System.out.println("Contents of the Hash Table:");
            for (Map.Entry<BucketKey, List<Map<String, Object>>> entry : hashTable.entrySet()) {
                System.out.println("Bucket: " + entry.getKey().micHd + "_" + entry.getKey().month);
                System.out.println("Rows:");

                // Print up to 10 rows from each bucket
                int rowCount = 0;
                for (Map<String, Object> row : entry.getValue()) {
                    System.out.println(row);
                    rowCount++;
                    if (rowCount >= 10) {
                        break;
                    }
                }
                System.out.println();
            }

             */

        } catch (SQLException e) {
            e.printStackTrace();
        }


        // PROBING PHASE

        Set<String> visitedLLCPRows = new HashSet<>(); // Optional for debugging

        // Establish a connection to the database
        try (Connection connection2 = DriverManager.getConnection(llcpUrl, dbUsername, dbPassword)) {
            // Retrieve rows from the "LLCP" table
            String sql2 = "SELECT * FROM llcp";
            try (Statement statement2 = connection2.createStatement();
                 ResultSet resultSet2 = statement2.executeQuery(sql2)) {

                while (resultSet2.next()) {
                    int micHd = resultSet2.getInt("_MICHD"); // Get the _MICHD value
                    String month = resultSet2.getString("Timestamp").substring(5, 7); // Get the month
                    String rowKey = micHd + "_" + month; // Optional for printing results
                    Map<String, Object> row = new LinkedHashMap<>();

                    // Store all columns of the row
                    ResultSetMetaData metaData = resultSet2.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = resultSet2.getObject(i);
                        row.put(columnName, columnValue);
                    }

                    // Create a BucketKey object for the current row
                    BucketKey bucketKey = new BucketKey(micHd, month);

                    // Get all matching rows from the hash table
                    List<Map<String, Object>> matchingRows = hashTable.get(bucketKey);

                    // Increment a counter
                    matchingCount += matchingRows.size();


                    // All the commented lines below are for debugging and printing results

                    /*
                    if (!visitedLLCPRows.contains(rowKey)) {
                        BucketKey bucketKey = new BucketKey(micHd, month);
                        List<Map<String, Object>> matchingRows = hashTable.get(bucketKey);
                        matchingCount += matchingRows.size();

                        if (matchingRows != null) {
                            int matchCount = matchingRows.size();
                            System.out.println("LLCP row: _MICHD=" + micHd + ", Month=" + month);
                            System.out.println("Number of matching rows in MMSA: " + matchCount);
                            System.out.println();
                        }

                        visitedLLCPRows.add(rowKey);
                    }

                     */


                    /*
                    if (matchingRows != null) {
                        int matchCount = matchingRows.size();
                        System.out.println("LLCP row: _MICHD=" + micHd + ", Month=" + month);
                        System.out.println("Number of matching rows in MMSA: " + matchCount);
                        System.out.println();
                    }

                     */


                    /*
                    // If a match is found, print the joined rows
                    if (matchingRows != null) {
                        System.out.println("Match found for LLCP row: " + row);
                        int matchCount = 0;
                        for (Map<String, Object> matchingRow : matchingRows) {
                            System.out.println("Matching MMSA row: " + matchingRow);
                            matchCount++;
                            if (matchCount >= 10) {
                                break;
                            }
                        }
                        System.out.println();
                    }

                     */
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();

        long totalTimeInMillis = endTime - startTime;

        double totalTimeInSeconds = totalTimeInMillis / 1000.0;

        System.out.println("Total execution time: " + totalTimeInSeconds + " seconds");

        System.out.println("Matching count: " + matchingCount);
    }
}
