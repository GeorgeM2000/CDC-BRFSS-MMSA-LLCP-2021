import java.sql.*;
import java.util.*;

public class SemiJoin {


    static class SemiBucketKey {
        int micHd;
        String month;
        int hcvu652;
        int rfhype6;
        int rfchol3;


        public SemiBucketKey(int micHd, String month, int hcvu652, int rfhype6, int rfchol3) {
            this.micHd = micHd;
            this.month = month;
            this.hcvu652 = hcvu652;
            this.rfhype6 = rfhype6;
            this.rfchol3 = rfchol3;
        }

        public SemiBucketKey() {}

        // Override equals and hashCode methods to ensure correct behavior when using BucketKey as a key in a HashMap
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SemiBucketKey bucketKey = (SemiBucketKey) o;
            return micHd == bucketKey.micHd && Objects.equals(month, bucketKey.month) && hcvu652 == bucketKey.hcvu652 && rfhype6 == bucketKey.rfhype6 && rfchol3 == bucketKey.rfchol3;
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

        public int getHcvu652() {
            return hcvu652;
        }

        public int getRfhype6() {
            return rfhype6;
        }

        public int getRfchol3() {
            return rfchol3;
        }
    }

    public static void main(String[] args) {
        String mmsa = "CDC_BRFSS_Datasets/MMSA_2021_Undersampled_VerB.csv";
        String llcp = "CDC_BRFSS_Datasets/LLCP_2021_Undersampled_VerB.csv";

        String mmsaUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_MMSA";
        String llcpUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_LLCP";
        String dbUsername = "root";
        String dbPassword = "---";


        int[] micHdValues = {1, 2};
        int[] hcvu652Values = {1, 2};
        int[] rfhype6Values = {1, 2};
        int[] rfchol3Values = {1, 2};
        String[] months = {"01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"};


        Map<SemiBucketKey, List<Map<String, Object>>> mmsaHashTable = new HashMap<>();
        Map<SemiBucketKey, List<Map<String, Object>>> llcpHashTable = new HashMap<>();


        // Generate all possible combinations
        for (int micHd : micHdValues) {
            for (int hcvu652 : hcvu652Values) {
                for (int rfhype6 : rfhype6Values) {
                    for (int rfchol3 : rfchol3Values) {
                        for (String month : months) {
                            SemiBucketKey key = new SemiBucketKey(micHd, month, hcvu652, rfhype6, rfchol3);
                            mmsaHashTable.put(key, new ArrayList<>());
                            llcpHashTable.put(key, new ArrayList<>());

                        }
                    }
                }
            }
        }

        System.out.println("Hash Table size: " + mmsaHashTable.size());


        int matchingCount = 0;
        List<Map<String, Object>> matchedRows = new ArrayList<>();

        try (Connection conn1 = DriverManager.getConnection(mmsaUrl, dbUsername, dbPassword);
             Connection conn2 = DriverManager.getConnection(llcpUrl, dbUsername, dbPassword);
             Statement stmt1 = conn1.createStatement();
             Statement stmt2 = conn2.createStatement();
             ResultSet rsMmsa = stmt1.executeQuery("SELECT * FROM mmsa");
             ResultSet rsLlcp = stmt2.executeQuery("SELECT * FROM llcp")) {


            Random rand = new Random();
            while (rsMmsa.next() || rsLlcp.next()) {
                if (rsMmsa.next() && rand.nextBoolean()) {
                    int micHd = rsMmsa.getInt("_MICHD");
                    String month = rsMmsa.getString("Timestamp").substring(5, 7);
                    int hcvu652 = rsMmsa.getInt("_HCVU652");
                    int rfhype6 = rsMmsa.getInt("_RFHYPE6");
                    int rfchol3 = rsMmsa.getInt("_RFCHOL3");
                    Map<String, Object> row = new LinkedHashMap<>();

                    // Store all columns of the row
                    ResultSetMetaData metaData = rsMmsa.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = rsMmsa.getObject(i);
                        row.put(columnName, columnValue);
                    }

                    SemiBucketKey bucketKey = new SemiBucketKey(micHd, month, hcvu652, rfhype6, rfchol3);
                    List<Map<String, Object>> rows = mmsaHashTable.computeIfAbsent(bucketKey, k -> new ArrayList<>());
                    rows.add(row);

                    // Probe LLCP hash table
                    if (llcpHashTable.containsKey(bucketKey)) {
                        matchingCount++;
                        matchedRows.add(row);
                    }
                } else if (rsLlcp.next()) {
                    int micHd = rsLlcp.getInt("_MICHD");
                    String month = rsLlcp.getString("Timestamp").substring(5, 7);
                    int hcvu652 = rsLlcp.getInt("_HCVU652");
                    int rfhype6 = rsLlcp.getInt("_RFHYPE6");
                    int rfchol3 = rsLlcp.getInt("_RFCHOL3");
                    Map<String, Object> row = new LinkedHashMap<>();

                    // Store all columns of the row
                    ResultSetMetaData metaData = rsLlcp.getMetaData();
                    int columnCount = metaData.getColumnCount();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object columnValue = rsLlcp.getObject(i);
                        row.put(columnName, columnValue);
                    }

                    SemiBucketKey bucketKey = new SemiBucketKey(micHd, month, hcvu652, rfhype6, rfchol3);
                    List<Map<String, Object>> rows = llcpHashTable.computeIfAbsent(bucketKey, k -> new ArrayList<>());
                    rows.add(row);

                }
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }


        System.out.println("Matching count: " + matchingCount);

        for (Map<String, Object> matchedRow : matchedRows) {
            System.out.println(matchedRow);
        }


    }
}
