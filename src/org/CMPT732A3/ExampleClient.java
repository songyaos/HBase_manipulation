package org.CMPT732A3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.*;

public class ExampleClient {
	public static void main(String[] args) throws IOException {
		Configuration config = HBaseConfiguration.create();
		// Create table
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor(
				TableName.valueOf("songyaos_testtable1"));//change it to your own new table name
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		admin.createTable(htd);
		byte[] tablename = htd.getName();
		HTableDescriptor[] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {

			throw new IOException("Failed create of table");

		}

		// Run some operations -- a put, and a get
		HTable table = new HTable(config, tablename);
		byte[] row1 = Bytes.toBytes("row1");
		Put p1 = new Put(row1);
		byte[] databytes = Bytes.toBytes("data");
		p1.add(databytes, Bytes.toBytes("col1"), Bytes.toBytes("value1"));
		p1.add(databytes, Bytes.toBytes("col2"), Bytes.toBytes("value2"));
		table.put(p1);

		// read avahdat_weather table entries
		HConnection connection = HConnectionManager.createConnection(config);
		HTableInterface exist_table = connection.getTable(TableName
				.valueOf("avahdat_weather"));
		// HTableDescriptor mydes = exist_table.getTableDescriptor();
		// Collection<HColumnDescriptor> families= mydes.getFamilies();
		// List<String> famList=new ArrayList<String>();
		// for ( HColumnDescriptor h : families) {
		//
		// famList.add(h.getNameAsString());
		// }

		try {
			// Filter filter = new QualifierFilter(CompareOp.EQUAL,new
			// BinaryComparator(Bytes.toBytes("SNOW")));
			SingleColumnValueFilter filter_col = new SingleColumnValueFilter(
					Bytes.toBytes("data"), Bytes.toBytes("SNOW"),
					CompareOp.GREATER,
					new BinaryComparator(Bytes.toBytes(1040)));
			RowFilter filter_usc = new RowFilter(CompareOp.GREATER,
					new BinaryComparator(Bytes.toBytes("USC")));
			filter_col.setFilterIfMissing(true);

			FilterList myfilterlist = new FilterList(Operator.MUST_PASS_ALL,
					Arrays.asList((Filter) filter_col, filter_usc));
			Scan scan = new Scan();
			scan.setFilter(myfilterlist);
			ResultScanner myscanner = exist_table.getScanner(scan);
			// ResultScanner myscanner = exist_table.getScanner(new Scan());
			System.out.println("scanning table:"
					+ new String(exist_table.getTableName()));
			int max_snow = 0;
			for (Result next_result = myscanner.next(); next_result != null; next_result = myscanner
					.next()) {
				max_snow = snow_scan(next_result, max_snow);
			}
			System.out.println(String.valueOf(max_snow));
			myscanner.close();
		} finally {

			table.close();
			connection.close();
		}

		// fix the output format to the following format
		// rowKey, columnFamily:qualifier, value
		// System.out.println("Get: " + result);
		Get g = new Get(row1);
		Result result = table.get(g);
		System.out.println("scanning firt row in table:"
				+ new String(table.getTableName()));
		result_format_scan(result);

		// Let's put more data the table
		byte[] row2 = Bytes.toBytes("row2");
		Put p2 = new Put(row2);
		p2.add(databytes, Bytes.toBytes("col1"), Bytes.toBytes("value3"));
		table.put(p2);
		byte[] row3 = Bytes.toBytes("row3");
		Put p3 = new Put(row3);
		p3.add(databytes, Bytes.toBytes("col2"), Bytes.toBytes("value4"));
		table.put(p3);
		// Write a scan on whole table.
		// Put your code here
		ResultScanner wholescanner = table.getScanner(new Scan());
		System.out.println("scanning full table:"
				+ new String(table.getTableName()));
		for (Result next_result = wholescanner.next(); next_result != null; next_result = wholescanner
				.next()) {
			result_format_scan(next_result);
		}

		wholescanner.close();
		table.close();

		// Drop the table
		admin.disableTable(tablename);
		admin.deleteTable(tablename);
	}

	public static void result_format_scan(Result result) {
		CellScanner scanner = result.cellScanner();
		Cell cell;
		try {
			while (scanner.advance()) {
				cell = scanner.current();
				String rownkey = Bytes.toString(CellUtil.cloneRow(cell));
				String valuestring = Bytes.toString(CellUtil.cloneValue(cell));

				String familystring = Bytes
						.toString(CellUtil.cloneFamily(cell));
				String qualifierstring = Bytes.toString(CellUtil
						.cloneQualifier(cell));
				System.out.println(rownkey + ",	" + familystring + ":"
						+ qualifierstring + ",	" + valuestring);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static int snow_scan(Result result, int max_snow) {
		CellScanner scanner = result.cellScanner();
		Cell cell;
		try {
			while (scanner.advance()) {
				cell = scanner.current();
				String qualifierstring = Bytes.toString(CellUtil
						.cloneQualifier(cell));
				if (qualifierstring.contains("SNOW")) {
					String valuestring = Bytes.toString(CellUtil
							.cloneValue(cell));
					int current_snow = Integer.parseInt(valuestring);
					if (current_snow > max_snow) {
						max_snow = current_snow;
						String rownkey = Bytes
								.toString(CellUtil.cloneRow(cell));
						String familystring = Bytes.toString(CellUtil
								.cloneFamily(cell));
						System.out.println(rownkey + ",	" + familystring + ":"
								+ qualifierstring + ",	" + valuestring);
					}
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
		return max_snow;

	}

	public static int snow_scan2(Result result, int max_snow) {
		CellScanner scanner = result.cellScanner();
		Cell cell;
		try {
			while (scanner.advance()) {
				cell = scanner.current();
				String qualifierstring = Bytes.toString(CellUtil
						.cloneQualifier(cell));
				String valuestring = Bytes.toString(CellUtil.cloneValue(cell));
				int current_snow = Integer.parseInt(valuestring);
				if (current_snow > max_snow) {
					max_snow = current_snow;
				}
				String rownkey = Bytes.toString(CellUtil.cloneRow(cell));
				String familystring = Bytes
						.toString(CellUtil.cloneFamily(cell));
				System.out.println(rownkey + ",	" + familystring + ":"
						+ qualifierstring + ",	" + valuestring);

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
		return max_snow;

	}
}