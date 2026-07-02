import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.batching.Batcher;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import com.google.cloud.bigtable.data.v2.models.TableId;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/*
 * Use Google Bigtable to store and analyze weather sensor data.
 * Row key design: STATION#YYYY-MM-DD#HH
 * Example: YVR#2022-10-01#10
 */
public class Bigtable {
    // TODO: update these before running in your own Google Cloud project.
    public final String projectId = "g25ai1008-ail7520-bigquery";
    public final String instanceId = "weather-instance";

    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather_ananya_g25ai1008";

    public BigtableDataClient dataClient;
    public BigtableTableAdminClient adminClient;

    public static void main(String[] args) throws Exception {
        Bigtable testbt = new Bigtable();
        testbt.run();
    }

    public void connect() throws IOException {
        dataClient = BigtableDataClient.create(projectId, instanceId);
        adminClient = BigtableTableAdminClient.create(projectId, instanceId);
        System.out.println("Connected to Bigtable instance: " + instanceId);
    }

    public void run() throws Exception {
        connect();

        // First run: keep these enabled to rebuild and load the table.
        // After data is loaded, comment out deleteTable(), createTable(), and loadData()
        // when you only want to re-run the queries.
        deleteTable();
        createTable();
        loadData();

        int temp = query1();
        System.out.println("Temperature: " + temp);

        int windspeed = query2();
        System.out.println("Windspeed: " + windspeed);

        ArrayList<Object[]> data = query3();
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < data.size(); i++) {
            Object[] vals = data.get(i);
            for (int j = 0; j < vals.length; j++) {
                buf.append(vals[j].toString()).append(" ");
            }
            buf.append("\n");
        }
        System.out.println(buf.toString());

        temp = query4();
        System.out.println("Temperature: " + temp);

        int myQueryResult = query5();
        System.out.println("Query5 result: " + myQueryResult);

        close();
    }

    /** Close data and admin clients. */
    public void close() {
        if (dataClient != null) {
            dataClient.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
    }

    public void createTable() {
        System.out.println("\nCreating table: " + tableId);
        if (adminClient.exists(tableId)) {
            System.out.println("Table already exists: " + tableId);
            return;
        }

        adminClient.createTable(
            CreateTableRequest.of(tableId)
                .addFamily(COLUMN_FAMILY)
        );
        System.out.println("Table created successfully: " + tableId);
    }

    /**
     * Loads data into Bigtable.
     * Data is in CSV files. Converts to hourly data by taking the first reading in each hour.
     */
    public void loadData() throws Exception {
        File dataDir = resolveDataDir();
        System.out.println("\nData directory: " + dataDir.getAbsolutePath());

        System.out.println("Load data for SeaTac");
        int seaCount = loadStationFile(new File(dataDir, "seatac.csv"), "SEA");

        System.out.println("Loading data for Vancouver");
        int yvrCount = loadStationFile(new File(dataDir, "vancouver.csv"), "YVR");

        System.out.println("Loading data for Portland");
        int pdxCount = loadStationFile(new File(dataDir, "portland.csv"), "PDX");

        System.out.println("Rows loaded: SEA=" + seaCount + ", YVR=" + yvrCount + ", PDX=" + pdxCount);
        System.out.println("Total hourly rows loaded: " + (seaCount + yvrCount + pdxCount));
    }

    /**
     * Query 1: temperature at Vancouver on 2022-10-01 at 10 a.m.
     */
    public int query1() throws Exception {
        System.out.println("\nExecuting query #1.");
        String rowKey = makeRowKey("YVR", "2022-10-01", "10");
        Row row = dataClient.readRow(TableId.of(tableId), rowKey);
        if (row == null) {
            throw new Exception("No row found for key: " + rowKey);
        }
        return parseIntCell(row, "temperature", -999);
    }

    /**
     * Query 2: highest windspeed in September 2022 in Portland.
     */
    public int query2() throws Exception {
        System.out.println("\nExecuting query #2.");
        int maxWindSpeed = 0;

        Query query = Query.create(TableId.of(tableId))
            .range("PDX#2022-09", "PDX#2022-10");

        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            int windSpeed = parseIntCell(row, "windspeed", -1);
            if (windSpeed > maxWindSpeed) {
                maxWindSpeed = windSpeed;
            }
        }
        return maxWindSpeed;
    }

    /**
     * Query 3: all readings for SeaTac for October 2, 2022.
     * Each Object[] has: date, hour, temperature, dewpoint, humidity, windspeed, pressure.
     */
    public ArrayList<Object[]> query3() throws Exception {
        System.out.println("\nExecuting query #3.");
        ArrayList<Object[]> data = new ArrayList<Object[]>();

        Query query = Query.create(TableId.of(tableId))
            .range("SEA#2022-10-02#", "SEA#2022-10-03#");

        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            Object[] record = new Object[] {
                getCellValue(row, "date"),
                getCellValue(row, "hour"),
                parseIntCell(row, "temperature", -999),
                parseIntCell(row, "dewpoint", -999),
                getCellValue(row, "humidity"),
                getCellValue(row, "windspeed"),
                getCellValue(row, "pressure")
            };
            data.add(record);
        }
        return data;
    }

    /**
     * Query 4: highest temperature at any station in July and August 2022.
     * This scans only station/date ranges for July-August, not the entire table.
     */
    public int query4() throws Exception {
        System.out.println("\nExecuting query #4.");
        int maxTemp = -100;

        Query query = Query.create(TableId.of(tableId))
            .range("PDX#2022-07", "PDX#2022-09")
            .range("SEA#2022-07", "SEA#2022-09")
            .range("YVR#2022-07", "YVR#2022-09");

        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            int temp = parseIntCell(row, "temperature", -999);
            if (temp > maxTemp) {
                maxTemp = temp;
            }
        }
        return maxTemp;
    }

    /**
     * Query 5: custom query - count SeaTac readings on 2022-10-02 with humidity >= 80.
     */
    public int query5() throws Exception {
        System.out.println("\nExecuting query #5.");
        int count = 0;

        Query query = Query.create(TableId.of(tableId))
            .range("SEA#2022-10-02#", "SEA#2022-10-03#");

        ServerStream<Row> rows = dataClient.readRows(query);
        for (Row row : rows) {
            double humidity = parseDoubleCell(row, "humidity", -1.0);
            if (humidity >= 80.0) {
                count++;
            }
        }
        return count;
    }

    /** Delete the table from Bigtable. */
    public void deleteTable() {
        System.out.println("\nDeleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.printf("Table %s deleted successfully%n", tableId);
        } catch (NotFoundException e) {
            System.err.println("Failed to delete a non-existent table: " + e.getMessage());
        }
    }

    private int loadStationFile(File file, String stationId) throws Exception {
        if (!file.exists()) {
            throw new IOException("Missing CSV file: " + file.getAbsolutePath());
        }

        int loaded = 0;
        int submitted = 0;
        Set<String> hourlyKeysSeen = new HashSet<String>();

        try (Batcher<RowMutationEntry, Void> batcher = dataClient.newBulkMutationBatcher(TableId.of(tableId));
             BufferedReader br = new BufferedReader(new FileReader(file))) {

            // Skip station title line and header line.
            br.readLine();
            br.readLine();

            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] values = line.split(",", -1);
                if (values.length < 9) {
                    continue;
                }

                String date = values[1].trim();
                String time = values[2].trim();
                if (date.isEmpty() || time.isEmpty()) {
                    continue;
                }

                String hour = normalizeHour(time);
                String rowKey = makeRowKey(stationId, date, hour);

                // The assignment says to keep only the first reading in each hour.
                if (hourlyKeysSeen.contains(rowKey)) {
                    continue;
                }
                hourlyKeysSeen.add(rowKey);

                String temperature = clean(values[3]);
                String dewpoint = clean(values[4]);
                String humidity = clean(values[5]);
                String windspeed = clean(values[6]);
                String pressure = clean(values[8]);

                RowMutationEntry mutation = RowMutationEntry.create(rowKey)
                    .setCell(COLUMN_FAMILY, "station", stationId)
                    .setCell(COLUMN_FAMILY, "date", date)
                    .setCell(COLUMN_FAMILY, "time", time)
                    .setCell(COLUMN_FAMILY, "hour", hour)
                    .setCell(COLUMN_FAMILY, "temperature", temperature)
                    .setCell(COLUMN_FAMILY, "dewpoint", dewpoint)
                    .setCell(COLUMN_FAMILY, "humidity", humidity)
                    .setCell(COLUMN_FAMILY, "windspeed", windspeed)
                    .setCell(COLUMN_FAMILY, "pressure", pressure);

                batcher.add(mutation);
                loaded++;
                submitted++;

                if (submitted % 1000 == 0) {
                    batcher.flush();
                    System.out.println("  " + stationId + " rows submitted: " + submitted);
                }
            }
            batcher.flush();
        }

        return loaded;
    }

    private File resolveDataDir() {
        String dataDirProperty = System.getProperty("data.dir");
        if (dataDirProperty != null && !dataDirProperty.trim().isEmpty()) {
            File dir = new File(dataDirProperty.trim());
            if (dir.exists()) {
                return dir;
            }
        }

        String[] candidates = new String[] {
            "data",
            "bin/data",
            "Assig-4/data",
            "src/main/resources/data"
        };

        for (String candidate : candidates) {
            File dir = new File(candidate);
            if (dir.exists() && dir.isDirectory()) {
                return dir;
            }
        }

        return new File("data");
    }

    private String makeRowKey(String station, String date, String hour) {
        return station + "#" + date + "#" + normalizeHour(hour);
    }

    private String normalizeHour(String timeOrHour) {
        String hour = timeOrHour.trim();
        if (hour.contains(":")) {
            hour = hour.substring(0, hour.indexOf(':'));
        }
        if (hour.length() == 1) {
            return "0" + hour;
        }
        return hour;
    }

    private String clean(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private String getCellValue(Row row, String qualifier) {
        if (row == null) {
            return "";
        }
        for (RowCell cell : row.getCells()) {
            if (COLUMN_FAMILY.equals(cell.getFamily())
                && qualifier.equals(cell.getQualifier().toStringUtf8())) {
                return cell.getValue().toStringUtf8();
            }
        }
        return "";
    }

    private int parseIntCell(Row row, String qualifier, int defaultValue) {
        String value = getCellValue(row, qualifier);
        if (value == null || value.trim().isEmpty() || "M".equalsIgnoreCase(value.trim())) {
            return defaultValue;
        }
        return (int) Math.round(Double.parseDouble(value.trim()));
    }

    private double parseDoubleCell(Row row, String qualifier, double defaultValue) {
        String value = getCellValue(row, qualifier);
        if (value == null || value.trim().isEmpty() || "M".equalsIgnoreCase(value.trim())) {
            return defaultValue;
        }
        return Double.parseDouble(value.trim());
    }
}
