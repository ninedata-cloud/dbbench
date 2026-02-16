package com.ninedata.dbbench.tpcc.loader;

import com.ninedata.dbbench.tpcc.TPCCUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.nio.file.Path;

/**
 * Generates TPC-C data as CSV files for database-native bulk loading.
 */
@Slf4j
public class CsvDataWriter {

    private static final String DELIMITER = ",";
    private static final String NULL_VALUE = "\\N";
    private static final ThreadLocal<SimpleDateFormat> TS_FORMAT =
            ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    // Column name arrays for each table (used by adapter.loadCsvFile)
    public static final String[] ITEM_COLUMNS = {"i_id", "i_im_id", "i_name", "i_price", "i_data"};
    public static final String[] WAREHOUSE_COLUMNS = {"w_id", "w_name", "w_street_1", "w_street_2",
            "w_city", "w_state", "w_zip", "w_tax", "w_ytd"};
    public static final String[] DISTRICT_COLUMNS = {"d_id", "d_w_id", "d_name", "d_street_1", "d_street_2",
            "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"};
    public static final String[] CUSTOMER_COLUMNS = {"c_id", "c_d_id", "c_w_id", "c_first", "c_middle",
            "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since",
            "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt",
            "c_delivery_cnt", "c_data"};
    public static final String[] STOCK_COLUMNS = {"s_i_id", "s_w_id", "s_quantity",
            "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05",
            "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10",
            "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data"};
    public static final String[] HISTORY_COLUMNS = {"h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id",
            "h_w_id", "h_date", "h_amount", "h_data"};
    public static final String[] ORDER_COLUMNS = {"o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d",
            "o_carrier_id", "o_ol_cnt", "o_all_local"};
    public static final String[] NEW_ORDER_COLUMNS = {"no_o_id", "no_d_id", "no_w_id"};
    public static final String[] ORDER_LINE_COLUMNS = {"ol_o_id", "ol_d_id", "ol_w_id", "ol_number",
            "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"};

    private final Path outputDir;

    public CsvDataWriter(Path outputDir) {
        this.outputDir = outputDir;
    }
    public String getFilePath(String tableName, int warehouseId) {
        return outputDir.resolve(tableName + "_w" + warehouseId + ".csv").toString();
    }

    public String getFilePath(String tableName) {
        return outputDir.resolve(tableName + ".csv").toString();
    }

    private String formatTimestamp() {
        return TS_FORMAT.get().format(new Timestamp(System.currentTimeMillis()));
    }

    private String csvField(String value) {
        if (value == null) return NULL_VALUE;
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private String makeOriginalData(String data) {
        if (TPCCUtil.randomInt(1, 100) <= 10) {
            int pos = TPCCUtil.randomInt(0, data.length() - 8);
            return data.substring(0, pos) + "ORIGINAL" + data.substring(pos + 8);
        }
        return data;
    }

    public void writeItems() throws IOException {
        String path = getFilePath("item");
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(path), StandardCharsets.UTF_8), 1 << 16)) {
            for (int i = 1; i <= TPCCUtil.ITEMS; i++) {
                w.write(String.valueOf(i));
                w.write(DELIMITER); w.write(String.valueOf(TPCCUtil.randomInt(1, 10000)));
                w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(14, 24)));
                w.write(DELIMITER); w.write(String.format("%.2f", TPCCUtil.randomDouble(1.00, 100.00)));
                w.write(DELIMITER); w.write(csvField(makeOriginalData(TPCCUtil.randomString(26, 50))));
                w.newLine();
            }
        }
    }
    public void writeWarehouse(int wId) throws IOException {
        String path = getFilePath("warehouse", wId);
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(path), StandardCharsets.UTF_8))) {
            w.write(String.valueOf(wId));
            w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(6, 10)));
            w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(10, 20)));
            w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(10, 20)));
            w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(10, 20)));
            w.write(DELIMITER); w.write(TPCCUtil.randomString(2, 2));
            w.write(DELIMITER); w.write(TPCCUtil.randomZip());
            w.write(DELIMITER); w.write(String.format("%.4f", TPCCUtil.randomDouble(0.0, 0.2)));
            w.write(DELIMITER); w.write("300000.00");
            w.newLine();
        }
    }

    public void writeDistricts(int wId) throws IOException {
        String path = getFilePath("district", wId);
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(path), StandardCharsets.UTF_8))) {
            for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
                w.write(String.valueOf(d));
                w.write(DELIMITER); w.write(String.valueOf(wId));
                w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(6, 10)));
                w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(10, 20)));
                w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(10, 20)));
                w.write(DELIMITER); w.write(csvField(TPCCUtil.randomString(10, 20)));
                w.write(DELIMITER); w.write(TPCCUtil.randomString(2, 2));
                w.write(DELIMITER); w.write(TPCCUtil.randomZip());
                w.write(DELIMITER); w.write(String.format("%.4f", TPCCUtil.randomDouble(0.0, 0.2)));
                w.write(DELIMITER); w.write("30000.00");
                w.write(DELIMITER); w.write("3001");
                w.newLine();
            }
        }
    }
    public void writeCustomers(int wId) throws IOException {
        String custPath = getFilePath("customer", wId);
        String histPath = getFilePath("history", wId);
        String ts = formatTimestamp();
        try (BufferedWriter cw = new BufferedWriter(new OutputStreamWriter(
                     new FileOutputStream(custPath), StandardCharsets.UTF_8), 1 << 16);
             BufferedWriter hw = new BufferedWriter(new OutputStreamWriter(
                     new FileOutputStream(histPath), StandardCharsets.UTF_8), 1 << 16)) {
            for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
                for (int c = 1; c <= TPCCUtil.CUSTOMERS_PER_DISTRICT; c++) {
                    String lastName = c <= 1000
                            ? TPCCUtil.generateLastName(c - 1)
                            : TPCCUtil.generateLastName(TPCCUtil.NURand(255, 0, 999));
                    cw.write(String.valueOf(c));
                    cw.write(DELIMITER); cw.write(String.valueOf(d));
                    cw.write(DELIMITER); cw.write(String.valueOf(wId));
                    cw.write(DELIMITER); cw.write(csvField(TPCCUtil.randomString(8, 16)));
                    cw.write(DELIMITER); cw.write("OE");
                    cw.write(DELIMITER); cw.write(csvField(lastName));
                    cw.write(DELIMITER); cw.write(csvField(TPCCUtil.randomString(10, 20)));
                    cw.write(DELIMITER); cw.write(csvField(TPCCUtil.randomString(10, 20)));
                    cw.write(DELIMITER); cw.write(csvField(TPCCUtil.randomString(10, 20)));
                    cw.write(DELIMITER); cw.write(TPCCUtil.randomString(2, 2));
                    cw.write(DELIMITER); cw.write(TPCCUtil.randomZip());
                    cw.write(DELIMITER); cw.write(TPCCUtil.randomNumericString(16));
                    cw.write(DELIMITER); cw.write(ts);
                    cw.write(DELIMITER); cw.write(TPCCUtil.randomInt(1, 100) <= 10 ? "BC" : "GC");
                    cw.write(DELIMITER); cw.write("50000.00");
                    cw.write(DELIMITER); cw.write(String.format("%.4f", TPCCUtil.randomDouble(0.0, 0.5)));
                    cw.write(DELIMITER); cw.write("-10.00");
                    cw.write(DELIMITER); cw.write("10.00");
                    cw.write(DELIMITER); cw.write("1");
                    cw.write(DELIMITER); cw.write("0");
                    cw.write(DELIMITER); cw.write(csvField(TPCCUtil.randomString(300, 500)));
                    cw.newLine();

                    hw.write(String.valueOf(c));
                    hw.write(DELIMITER); hw.write(String.valueOf(d));
                    hw.write(DELIMITER); hw.write(String.valueOf(wId));
                    hw.write(DELIMITER); hw.write(String.valueOf(d));
                    hw.write(DELIMITER); hw.write(String.valueOf(wId));
                    hw.write(DELIMITER); hw.write(ts);
                    hw.write(DELIMITER); hw.write("10.00");
                    hw.write(DELIMITER); hw.write(csvField(TPCCUtil.randomString(12, 24)));
                    hw.newLine();
                }
            }
        }
    }
    public void writeStock(int wId) throws IOException {
        String path = getFilePath("stock", wId);
        try (BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(path), StandardCharsets.UTF_8), 1 << 16)) {
            for (int i = 1; i <= TPCCUtil.ITEMS; i++) {
                w.write(String.valueOf(i));
                w.write(DELIMITER); w.write(String.valueOf(wId));
                w.write(DELIMITER); w.write(String.valueOf(TPCCUtil.randomInt(10, 100)));
                for (int j = 0; j < 10; j++) {
                    w.write(DELIMITER); w.write(TPCCUtil.randomString(24, 24));
                }
                w.write(DELIMITER); w.write("0");
                w.write(DELIMITER); w.write("0");
                w.write(DELIMITER); w.write("0");
                w.write(DELIMITER); w.write(csvField(makeOriginalData(TPCCUtil.randomString(26, 50))));
                w.newLine();
            }
        }
    }
    public void writeOrders(int wId) throws IOException {
        String orderPath = getFilePath("oorder", wId);
        String newOrderPath = getFilePath("new_order", wId);
        String olPath = getFilePath("order_line", wId);
        String ts = formatTimestamp();

        try (BufferedWriter ow = new BufferedWriter(new OutputStreamWriter(
                     new FileOutputStream(orderPath), StandardCharsets.UTF_8), 1 << 16);
             BufferedWriter nw = new BufferedWriter(new OutputStreamWriter(
                     new FileOutputStream(newOrderPath), StandardCharsets.UTF_8), 1 << 16);
             BufferedWriter olw = new BufferedWriter(new OutputStreamWriter(
                     new FileOutputStream(olPath), StandardCharsets.UTF_8), 1 << 16)) {

            for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
                int[] cPerm = generateCustomerPermutation();
                for (int o = 1; o <= TPCCUtil.ORDERS_PER_DISTRICT; o++) {
                    int olCnt = TPCCUtil.randomInt(5, 15);
                    ow.write(String.valueOf(o));
                    ow.write(DELIMITER); ow.write(String.valueOf(d));
                    ow.write(DELIMITER); ow.write(String.valueOf(wId));
                    ow.write(DELIMITER); ow.write(String.valueOf(cPerm[o - 1]));
                    ow.write(DELIMITER); ow.write(ts);
                    ow.write(DELIMITER);
                    if (o < 2101) {
                        ow.write(String.valueOf(TPCCUtil.randomInt(1, 10)));
                    } else {
                        ow.write(NULL_VALUE);
                    }
                    ow.write(DELIMITER); ow.write(String.valueOf(olCnt));
                    ow.write(DELIMITER); ow.write("1");
                    ow.newLine();

                    if (o >= 2101) {
                        nw.write(String.valueOf(o));
                        nw.write(DELIMITER); nw.write(String.valueOf(d));
                        nw.write(DELIMITER); nw.write(String.valueOf(wId));
                        nw.newLine();
                    }

                    for (int ol = 1; ol <= olCnt; ol++) {
                        olw.write(String.valueOf(o));
                        olw.write(DELIMITER); olw.write(String.valueOf(d));
                        olw.write(DELIMITER); olw.write(String.valueOf(wId));
                        olw.write(DELIMITER); olw.write(String.valueOf(ol));
                        olw.write(DELIMITER); olw.write(String.valueOf(TPCCUtil.randomInt(1, TPCCUtil.ITEMS)));
                        olw.write(DELIMITER); olw.write(String.valueOf(wId));
                        olw.write(DELIMITER);
                        if (o < 2101) {
                            olw.write(ts);
                        } else {
                            olw.write(NULL_VALUE);
                        }
                        olw.write(DELIMITER); olw.write("5");
                        olw.write(DELIMITER); olw.write(String.format("%.2f",
                                o < 2101 ? 0.00 : TPCCUtil.randomDouble(0.01, 9999.99)));
                        olw.write(DELIMITER); olw.write(TPCCUtil.randomString(24, 24));
                        olw.newLine();
                    }
                }
            }
        }
    }

    private int[] generateCustomerPermutation() {
        int[] perm = new int[TPCCUtil.CUSTOMERS_PER_DISTRICT];
        for (int i = 0; i < perm.length; i++) perm[i] = i + 1;
        java.util.concurrent.ThreadLocalRandom rnd = java.util.concurrent.ThreadLocalRandom.current();
        for (int i = perm.length - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int tmp = perm[i]; perm[i] = perm[j]; perm[j] = tmp;
        }
        return perm;
    }
}
