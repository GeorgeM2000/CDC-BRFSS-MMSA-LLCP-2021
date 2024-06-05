import java.sql.*;
import java.util.*;
import java.util.function.Supplier;

public class MergeSortJoin {

    // The interface and classes below are used to handle the rows of the MMSA and LLCP tables

    interface TableRow {
        String getTimestamp();

        int get_MICHD();
    }

    static class LLCPRow implements TableRow {
        private int _MICHD;
        private String Timestamp;

        public LLCPRow(int _MICHD, String timestamp) {
            this._MICHD = _MICHD;
            Timestamp = timestamp;
        }

        public LLCPRow() {

        }

        public void set_MICHD(int _MICHD) {
            this._MICHD = _MICHD;
        }

        public void setTimestamp(String timestamp) {
            Timestamp = timestamp;
        }

        public int get_MICHD() {
            return _MICHD;
        }

        public String getTimestamp() {
            return Timestamp;
        }
    }

    static class MMSARow implements TableRow {
        private int _MICHD;
        private String Timestamp;

        public MMSARow(int _MICHD, String timestamp) {
            this._MICHD = _MICHD;
            Timestamp = timestamp;
        }

        public MMSARow() {
        }

        public void set_MICHD(int _MICHD) {
            this._MICHD = _MICHD;
        }

        public void setTimestamp(String timestamp) {
            Timestamp = timestamp;
        }

        public int get_MICHD() {
            return _MICHD;
        }

        public String getTimestamp() {
            return Timestamp;
        }

    }

    // Method to load data from the MMSA or LLCP tables
    static <T extends TableRow> List<T> loadDataFromTable(String jdbcUrl, String username, String password, String tableName, Supplier<T> rowSupplier) {
        List<T> rows = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {

            while (rs.next()) {
                // Create LLCPRow or MMSARow object and add it to the list
                T row = rowSupplier.get();

                if (row instanceof LLCPRow) {
                    ((LLCPRow) row).set_MICHD(rs.getInt("_MICHD"));
                    ((LLCPRow) row).setTimestamp(rs.getString("Timestamp"));
                } else if (row instanceof MMSARow) {
                    ((MMSARow) row).set_MICHD(rs.getInt("_MICHD"));
                    ((MMSARow) row).setTimestamp(rs.getString("Timestamp"));
                }
                // Add the row to the list
                rows.add(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rows;
    }

    // Method to sort rows based on _MICHD and month
    static <T extends TableRow> void sortRows(List<T> rows) {
        Collections.sort(rows, new Comparator<T>() {
            @Override
            public int compare(T row1, T row2) {
                // Compare _MICHD values
                int micHdComparison = Integer.compare(row1.get_MICHD(), row2.get_MICHD());
                if (micHdComparison != 0) {
                    return micHdComparison;
                }

                // Extract and compare month from timestamp
                String month1 = row1.getTimestamp().substring(5, 7);
                String month2 = row2.getTimestamp().substring(5, 7);
                return month1.compareTo(month2);
            }
        });
    }

    static <T extends TableRow> void printRows(List<T> rows, int n) {
        for (int i = 0; i < Math.min(rows.size(), n); i++) {
            T row = rows.get(i);
            System.out.println("Row " + (i + 1) + ": _MICHD=" + row.get_MICHD() + ", Timestamp=" + row.getTimestamp());
        }
    }

    // Method to convert the month to an integer
    public static int convertMonthToInt(String monthStr) {
        char firstChar = monthStr.charAt(0);
        char secondChar = monthStr.charAt(1);

        int firstDigit = Character.getNumericValue(firstChar);
        int secondDigit = Character.getNumericValue(secondChar);

        if (firstDigit == 0) {
            return firstDigit + secondDigit;
        } else {
            return Integer.parseInt("" + firstDigit + secondDigit);
        }
    }


    public static int performMergeJoin(List<LLCPRow> llcpRows, List<MMSARow> mmsaRows) {
        int llcpIndex = 0;
        int mmsaIndex = 0;
        int matchingCount = 0;

        while (mmsaIndex < mmsaRows.size() && llcpIndex < llcpRows.size()) {
            LLCPRow llcpRow = llcpRows.get(llcpIndex);
            MMSARow mmsaRow = mmsaRows.get(mmsaIndex);

            if (mmsaRow.get_MICHD() == llcpRow.get_MICHD()) {
                String llcpMonth = llcpRow.getTimestamp().substring(5, 7);
                String mmsaMonth = mmsaRow.getTimestamp().substring(5, 7);

                int llcpMonthInt = convertMonthToInt(llcpMonth);
                int mmsaMonthInt = convertMonthToInt(mmsaMonth);

                if (llcpMonthInt == mmsaMonthInt) {

                    while ((mmsaIndex < mmsaRows.size()) && (mmsaRows.get(mmsaIndex).get_MICHD() == llcpRows.get(llcpIndex).get_MICHD()
                            && mmsaRows.get(mmsaIndex).getTimestamp().substring(5, 7).equals(llcpRows.get(llcpIndex).getTimestamp().substring(5, 7)))) {

                        int tempLlcpIndex = llcpIndex;

                        while ((tempLlcpIndex < llcpRows.size()) && (mmsaRows.get(mmsaIndex).get_MICHD() == llcpRows.get(tempLlcpIndex).get_MICHD()
                                && mmsaRows.get(mmsaIndex).getTimestamp().substring(5, 7).equals(llcpRows.get(tempLlcpIndex).getTimestamp().substring(5, 7)))) {

                            matchingCount++;
                            tempLlcpIndex++;
                        }
                        mmsaIndex++;
                    }

                } else if (mmsaMonthInt < llcpMonthInt) {
                    mmsaIndex++;
                } else {
                    llcpIndex++;
                }
            } else if (mmsaRow.get_MICHD() < llcpRow.get_MICHD()) {
                mmsaIndex++;
            } else {
                llcpIndex++;
            }
        }
        return matchingCount;
    }


    public static void main(String[] args) {
        String mmsa = "CDC_BRFSS_Datasets/MMSA_2021_Undersampled_VerB.csv";
        String llcp = "CDC_BRFSS_Datasets/LLCP_2021_Undersampled_VerB.csv";

        String mmsaUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_MMSA";
        String llcpUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_LLCP";
        String dbUsername = "root";
        String dbPassword = "---";

        /*
        Merge-Sort-Join Algorithm
         */

        long startTime = System.currentTimeMillis();

        // Load data from LLCP and MMSA tables
        List<LLCPRow> llcpRows = loadDataFromTable(llcpUrl, dbUsername, dbPassword, "llcp", (Supplier<LLCPRow>) LLCPRow::new);
        List<MMSARow> mmsaRows = loadDataFromTable(mmsaUrl, dbUsername, dbPassword, "mmsa", (Supplier<MMSARow>) MMSARow::new);

        // Print first 10 rows of LLCP table
        /*
        System.out.println("LLCP Table:");
        for (int i = 0; i < Math.min(llcpRows.size(), 10); i++) {
            LLCPRow row = llcpRows.get(i);
            System.out.println("Row " + (i + 1) + ": _MICHD=" + row.get_MICHD() + ", Timestamp=" + row.getTimestamp());
        }

        // Print first 10 rows of MMSA table
        System.out.println("\nMMSA Table:");
        for (int i = 0; i < Math.min(mmsaRows.size(), 10); i++) {
            MMSARow row = mmsaRows.get(i);
            System.out.println("Row " + (i + 1) + ": _MICHD=" + row.get_MICHD() + ", Timestamp=" + row.getTimestamp());
        }

         */

        // Sort LLCP and MMSA rows based on _MICHD and month
        sortRows(llcpRows);
        sortRows(mmsaRows);

        //System.out.println("Number of MMSA rows: " + mmsaRows.size());
        //System.out.println("Number of LLCP rows: " + llcpRows.size());


        // Perform Merge-Sort Join
        int matchingCount = performMergeJoin(llcpRows, mmsaRows);

        long endTime = System.currentTimeMillis();

        long totalTimeInMillis = endTime - startTime;

        double totalTimeInSeconds = totalTimeInMillis / 1000.0;

        System.out.println("Total execution time: " + totalTimeInSeconds + " seconds");


        System.out.println("Total matching rows: " + matchingCount);

    }
}
