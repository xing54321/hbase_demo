package top.lbing.hbase_demo.controller;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hbase")
public class PrePartitionController {
	private static final Logger log = LoggerFactory.getLogger(PrePartitionController.class);

	@Autowired
	private Connection connection;

	@GetMapping("/isAlive")
	public String isAlive() {
		return "isAlive";
	}
	
	@GetMapping("/prePartition")
	public String prePartition() {
		List<String> columnFamilies = new ArrayList<>();
		columnFamilies.add("info");
		return createTableBySplitKeys("test", columnFamilies) ? "success" : "failed";
	}

	@GetMapping("/createData")
	public void createData() {
		log.info(getRandomNumber());
		for (int i = 0; i < 10000; i++) {
			batchPut();
		}
		log.info("createData success");
	}

	public boolean createTableBySplitKeys(String tableName, List<String> columnFamily) {
		try {
			if (StringUtils.isBlank(tableName) || columnFamily == null || columnFamily.size() < 0) {
				log.error("tableName|columnFamily不为null");
			}
			Admin admin = connection.getAdmin();
			TableName tableName1 = TableName.valueOf(tableName);
			if (admin.tableExists(tableName1)) {
				return true;
			} else {
				TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName1);
				for (String cf : columnFamily) {
					builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build());
				}
				byte[][] splitKeys = getSplitKeys();
				admin.createTable(builder.build(), splitKeys);// 指定splitkeys
				log.info("表" + tableName + "创建成功，列族：" + columnFamily.toString());
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 获取分区
	 * 
	 * @return
	 */
	private byte[][] getSplitKeys() {
		String[] keys = new String[] { "00_", "10_", "20_", "30_", "40_", "50_", "60_", "70_", "80_", "90_" };
		byte[][] splitKeys = new byte[keys.length][];
		TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);// 升序排序
		for (int i = 0; i < keys.length; i++) {
			rows.add(Bytes.toBytes(keys[i]));
		}
		Iterator<byte[]> rowKeyIter = rows.iterator();
		int i = 0;
		while (rowKeyIter.hasNext()) {
			byte[] tempRow = rowKeyIter.next();
			rowKeyIter.remove();
			splitKeys[i] = tempRow;
			i++;
		}
		return splitKeys;
	}

	private String getRandomNumber() {
		String ranStr = Math.random() + "";
		int pointIndex = ranStr.indexOf(".");
		return ranStr.substring(pointIndex + 1, pointIndex + 3);
	}

	private List<Put> batchPut() {
		List<Put> list = new ArrayList<Put>();
		for (int i = 1; i <= 10000; i++) {
			byte[] rowkey = Bytes.toBytes(getRandomNumber() + "-" + System.currentTimeMillis() + "-" + i);
			Put put = new Put(rowkey);
			put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zs" + i));
			list.add(put);
		}
		return list;
	}

}
