package top.lbing.hbase_demo.utils;

import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;

public class Tools {
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
}
