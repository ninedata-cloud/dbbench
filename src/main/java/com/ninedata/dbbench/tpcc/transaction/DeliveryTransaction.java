package com.ninedata.dbbench.tpcc.transaction;

import com.ninedata.dbbench.database.DatabaseAdapter;
import com.ninedata.dbbench.engine.TerminalContext;
import com.ninedata.dbbench.tpcc.TPCCUtil;

import java.sql.*;

public class DeliveryTransaction extends AbstractTransaction {

    public DeliveryTransaction(DatabaseAdapter adapter, int warehouseId, int districtId) {
        super(adapter, warehouseId, districtId);
    }

    @Override
    public String getName() {
        return "DELIVERY";
    }

    @Override
    protected boolean doExecute(TerminalContext ctx) throws SQLException {
        Connection conn = ctx.getConnection();
        int carrierId = TPCCUtil.randomInt(1, 10);
        Timestamp deliveryDate = new Timestamp(System.currentTimeMillis());
        int delivered = 0;

        for (int d = 1; d <= TPCCUtil.DISTRICTS_PER_WAREHOUSE; d++) {
            // Get oldest undelivered order
            int orderId;
            String sql = buildSelectFirstRowForUpdateQuery(
                "SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id");
            PreparedStatement ps = ctx.prepareStatement(sql);
            ps.setInt(1, warehouseId);
            ps.setInt(2, d);
            ResultSet rs = ps.executeQuery();
            if (!rs.next()) { rs.close(); continue; }
            orderId = rs.getInt(1);
            rs.close();

            // Delete from new_order
            ps = ctx.prepareStatement("DELETE FROM new_order WHERE no_w_id = ? AND no_d_id = ? AND no_o_id = ?");
            ps.setInt(1, warehouseId);
            ps.setInt(2, d);
            ps.setInt(3, orderId);
            ps.executeUpdate();

            // Get customer ID
            int customerId;
            ps = ctx.prepareStatement("SELECT o_c_id FROM oorder WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
            ps.setInt(1, warehouseId);
            ps.setInt(2, d);
            ps.setInt(3, orderId);
            rs = ps.executeQuery();
            if (!rs.next()) { rs.close(); continue; }
            customerId = rs.getInt(1);
            rs.close();

            // Update order carrier
            ps = ctx.prepareStatement("UPDATE oorder SET o_carrier_id = ? WHERE o_w_id = ? AND o_d_id = ? AND o_id = ?");
            ps.setInt(1, carrierId);
            ps.setInt(2, warehouseId);
            ps.setInt(3, d);
            ps.setInt(4, orderId);
            ps.executeUpdate();

            // Get total amount
            double totalAmount = 0;
            ps = ctx.prepareStatement("SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
            ps.setInt(1, warehouseId);
            ps.setInt(2, d);
            ps.setInt(3, orderId);
            rs = ps.executeQuery();
            if (rs.next()) {
                totalAmount = rs.getDouble(1);
            }
            rs.close();

            // Update order lines delivery date
            ps = ctx.prepareStatement("UPDATE order_line SET ol_delivery_d = ? WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?");
            ps.setTimestamp(1, deliveryDate);
            ps.setInt(2, warehouseId);
            ps.setInt(3, d);
            ps.setInt(4, orderId);
            ps.executeUpdate();

            // Update customer balance
            ps = ctx.prepareStatement("UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?");
            ps.setDouble(1, totalAmount);
            ps.setInt(2, warehouseId);
            ps.setInt(3, d);
            ps.setInt(4, customerId);
            ps.executeUpdate();

            delivered++;
        }

        return delivered > 0;
    }
}
