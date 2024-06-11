import java.sql.*;
import java.util.*;

public class PipelinedHashJoin {

    public static void main(String[] args) {
        String mmsaUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_MMSA";
        String llcpUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_LLCP";
        String dbUsername = "root";
        String dbPassword = "--";


        long startTime = System.currentTimeMillis();

        // Possible values for the _MICHD and month columns
        int[] micHdValues = {1, 2};
        String[] months = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};

        // Define a hash table to store rows based on the combination of values
        Map<SingleHashJoin.BucketKey, List<Map<String, Object>>> mmsaHashTable = new HashMap<>();
        Map<SingleHashJoin.BucketKey, List<Map<String, Object>>> llcpHashTable = new HashMap<>();

        // Populate the hash tables
        for (int micHd : micHdValues) {
            for (String month : months) {
                SingleHashJoin.BucketKey bucketKey = new SingleHashJoin.BucketKey(micHd, month);
                mmsaHashTable.put(bucketKey, new ArrayList<>());
                llcpHashTable.put(bucketKey, new ArrayList<>());
            }
        }

        // Print the buckets of one of the hash tables
        System.out.println("Number of Buckets: " + mmsaHashTable.size());
        for (Map.Entry<SingleHashJoin.BucketKey, List<Map<String, Object>>> entry : mmsaHashTable.entrySet()) {
            System.out.println("Bucket: " + entry.getKey().micHd + "_" + entry.getKey().month);
        }

        int matchingCount = 0;

        // Establish a connection
        try (Connection conn1 = DriverManager.getConnection(mmsaUrl, dbUsername, dbPassword);
             Connection conn2 = DriverManager.getConnection(llcpUrl, dbUsername, dbPassword);
             Statement stmt1 = conn1.createStatement();
             Statement stmt2 = conn2.createStatement();
             ResultSet rsMmsa = stmt1.executeQuery("SELECT * FROM mmsa");
             ResultSet rsLlcp = stmt2.executeQuery("SELECT * FROM llcp")) {


            Random rand = new Random(); // Based on this variable, either an MMSA row, or an LLCP row will be retrieved
            while (rsMmsa.next() || rsLlcp.next()) {

                if (rsMmsa.next() && rand.nextBoolean()) {

                    int micHd = rsMmsa.getInt("_MICHD");
                    String month = rsMmsa.getString("Timestamp").substring(5, 7);
                    Map<String, Object> row = new LinkedHashMap<>();

                    // Store all columns of the row
                    ResultSetMetaData metaData = rsMmsa.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = rsMmsa.getObject(i);
                        row.put(columnName, columnValue);
                    }

                    SingleHashJoin.BucketKey bucketKey = new SingleHashJoin.BucketKey(micHd, month);
                    List<Map<String, Object>> rows = mmsaHashTable.computeIfAbsent(bucketKey, k -> new ArrayList<>());
                    rows.add(row);

                    // Probe LLCP hash table
                    if (llcpHashTable.containsKey(bucketKey)) { // If a bucket key exists

                        // Increment the matching rows
                        List<Map<String, Object>> matchingRows = llcpHashTable.get(bucketKey);
                        matchingCount += matchingRows.size();
                    }
                } else if (rsLlcp.next()) {

                    int micHd = rsLlcp.getInt("_MICHD");
                    String month = rsLlcp.getString("Timestamp").substring(5, 7);
                    Map<String, Object> row = new LinkedHashMap<>();

                    // Store all columns of the row
                    ResultSetMetaData metaData = rsLlcp.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = rsLlcp.getObject(i);
                        row.put(columnName, columnValue);
                    }


                    SingleHashJoin.BucketKey bucketKey = new SingleHashJoin.BucketKey(micHd, month);
                    List<Map<String, Object>> rows = llcpHashTable.computeIfAbsent(bucketKey, k -> new ArrayList<>());
                    rows.add(row);


                    // Probe MMSA hash table
                    if (mmsaHashTable.containsKey(bucketKey)) {
                        List<Map<String, Object>> matchingRows = mmsaHashTable.get(bucketKey);
                        matchingCount += matchingRows.size();
                    }
                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Matching count: " + matchingCount);

    }

}
