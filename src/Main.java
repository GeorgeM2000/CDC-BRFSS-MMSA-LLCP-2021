import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.function.Supplier;


public class Main {

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


    public static void main(String[] args) {
        String mmsa = "CDC_BRFSS_Datasets/MMSA_2021_Undersampled_VerB.csv";
        String llcp = "CDC_BRFSS_Datasets/LLCP_2021_Undersampled_VerB.csv";

        String mmsaUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_MMSA";
        String llcpUrl = "jdbc:mysql://localhost:3306/CDC_BRFSS_LLCP";
        String dbUsername = "root";
        String dbPassword = "gmaok25m8102000DIS@ email";

        String timestamp1 = "2023-05-17";
        String timestamp2 = "2023-12-20";

        // Extract month parts
        String llcpMonth = timestamp1.substring(5, 7);
        String mmsaMonth = timestamp2.substring(5, 7);

        // Convert months to integers as per the requirement
        int llcpMonthInt = convertMonthToInt(llcpMonth);
        int mmsaMonthInt = convertMonthToInt(mmsaMonth);

        // Print the results
        System.out.println("Converted month for timestamp1: " + llcpMonthInt);
        System.out.println("Converted month for timestamp2: " + mmsaMonthInt);

        // Compare months
        if (llcpMonthInt == mmsaMonthInt) {
            System.out.println("The months are equal.");
        } else {
            System.out.println("The months are not equal.");
        }

        /*
        // === Populate the MMSA table ===

        try (Connection conn = DriverManager.getConnection(mmsaUrl, dbUsername, dbPassword);
             FileReader reader = new FileReader(mmsa);
             CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            int eid = 1;
            for (CSVRecord csvRecord : csvParser) {
                String _RFHLTH = csvRecord.get("_RFHLTH");
                String _HLTHPLN = csvRecord.get("_HLTHPLN");
                String _HCVU652 = csvRecord.get("_HCVU652");
                String _RFHYPE6 = csvRecord.get("_RFHYPE6");
                String _CHOLCH3 = csvRecord.get("_CHOLCH3");
                String _RFCHOL3 = csvRecord.get("_RFCHOL3");
                String _INCOMG1 = csvRecord.get("_INCOMG1");
                String _AGE_G = csvRecord.get("_AGE_G");
                String _MICHD = csvRecord.get("_MICHD");
                String Timestamp = csvRecord.get("Timestamp");

                // Insert data into database
                String sql = "INSERT INTO mmsa (eid, _RFHLTH, _HLTHPLN, _HCVU652, _RFHYPE6, _CHOLCH3, _RFCHOL3, _INCOMG1, _AGE_G, _MICHD, Timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = conn.prepareStatement(sql)) {
                    statement.setString(1, String.valueOf(eid));
                    statement.setString(2, _RFHLTH);
                    statement.setString(3, _HLTHPLN);
                    statement.setString(4, _HCVU652);
                    statement.setString(5, _RFHYPE6);
                    statement.setString(6, _CHOLCH3);
                    statement.setString(7, _RFCHOL3);
                    statement.setString(8, _INCOMG1);
                    statement.setString(9, _AGE_G);
                    statement.setString(10, _MICHD);
                    statement.setString(11, Timestamp);
                    statement.executeUpdate();
                }
                eid += 1;
            }

            System.out.println("Data imported successfully!");

        } catch (IOException | SQLException ex) {
            ex.printStackTrace();
        }

         */




        // === Populate the LLCP table

        /*
        try (Connection conn = DriverManager.getConnection(llcpUrl, dbUsername, dbPassword);
             FileReader reader = new FileReader(llcp);
             CSVParser csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            int eid = 1;
            for (CSVRecord csvRecord : csvParser) {
                String _RFHLTH = csvRecord.get("_RFHLTH");
                String _HLTHPLN = csvRecord.get("_HLTHPLN");
                String _HCVU652 = csvRecord.get("_HCVU652");
                String _RFHYPE6 = csvRecord.get("_RFHYPE6");
                String _CHOLCH3 = csvRecord.get("_CHOLCH3");
                String _RFCHOL3 = csvRecord.get("_RFCHOL3");
                String _INCOMG1 = csvRecord.get("_INCOMG1");
                String _AGE_G = csvRecord.get("_AGE_G");
                String MARITAL = csvRecord.get("MARITAL");
                String RENTHOM1 = csvRecord.get("RENTHOM1");
                String EMPLOY1 = csvRecord.get("EMPLOY1");
                String _MICHD = csvRecord.get("_MICHD");
                String Timestamp = csvRecord.get("Timestamp");

                // Insert data into database
                String sql = "INSERT INTO llcp (eid, _RFHLTH, _HLTHPLN, _HCVU652, _RFHYPE6, _CHOLCH3, _RFCHOL3, _INCOMG1, _AGE_G, _MICHD, MARITAL, RENTHOM1, EMPLOY1, Timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = conn.prepareStatement(sql)) {
                    statement.setString(1, String.valueOf(eid));
                    statement.setString(2, _RFHLTH);
                    statement.setString(3, _HLTHPLN);
                    statement.setString(4, _HCVU652);
                    statement.setString(5, _RFHYPE6);
                    statement.setString(6, _CHOLCH3);
                    statement.setString(7, _RFCHOL3);
                    statement.setString(8, _INCOMG1);
                    statement.setString(9, _AGE_G);
                    statement.setString(10, _MICHD);
                    statement.setString(11, MARITAL);
                    statement.setString(12, RENTHOM1);
                    statement.setString(13, EMPLOY1);
                    statement.setString(14, Timestamp);
                    statement.executeUpdate();
                }
                eid += 1;
            }

            System.out.println("Data imported successfully!");

        } catch (IOException | SQLException ex) {
            ex.printStackTrace();
        }

         */



    }

}
