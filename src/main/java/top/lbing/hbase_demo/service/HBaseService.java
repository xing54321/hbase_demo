package top.lbing.hbase_demo.service;

import java.io.IOException;

import org.apache.directory.api.util.Strings;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class HBaseService {
	 private Logger log = LoggerFactory.getLogger(HBaseService.class);
	/**
	 * 管理员可以做表以及数据的增删改查功能
	 */
//	 @Autowired
//	private HBaseAdmin hBaseAdmin ;
	 @Autowired
	private Connection connection;
	 
	 public boolean tableExists(String tableName) {
	        Table table = null;
	        boolean tableExistsFlag = false;
	        Admin hBaseAdmin;
	        try {
	            table = connection.getTable(TableName.valueOf(tableName));
	            hBaseAdmin=connection.getAdmin();
	            tableExistsFlag = hBaseAdmin.tableExists(table.getName());
	        } catch (IOException e) {
	            log.error("IOException : {}", e.getMessage());
	            e.printStackTrace();
	        } finally {
	            closeTable(table);
	        }
	        return tableExistsFlag;
	    }
	 
	 /**
	  * 据rowkey查询hbase数据
	  */
	 public String getByRowkey(String tableName,String rowkey) {
		 boolean tableExists = tableExists(tableName);
	        if (!tableExists) {
	            log.info("TABLE_NOT_EXISTS_MSG {}"  , tableName);
	            return null;
	        }

	        byte[] rowKeyA = Strings.toByteArray(rowkey);
	        Result result = null;
	        Table table = null;
	        try {
	            table = connection.getTable(TableName.valueOf(tableName));
	            Get get = new Get(rowKeyA);
	            result = table.get(get);
	        } catch (IOException e) {
	            log.error("IOException : {}", e.getMessage());
	            e.printStackTrace();
	        } finally {
	            closeTable(table);
	        }
	        return result.toString();
	 }
	 
	 /**
	     * 关闭连接
	     *
	     * @param table 表名
	     */
	    private void closeTable(Table table) {
	        if (table != null) {
	            try {
	                table.close();
	            } catch (IOException e) {
	                log.error("close table {} error {}", table.getName(), e.getMessage());
	                e.printStackTrace();
	            }
	        } else {
	            log.info("table is null");
	        }
	    }
	 

	/**
	 * 创建表 create
	 * <table>
	 * , {NAME => <column family>, VERSIONS => <VERSIONS>}
	 */
	/*public boolean creatTable(String tableName, List<String> columnFamily) {
		try {
			// 列族column family
			List<ColumnFamilyDescriptor> cfDesc = new ArrayList<>(columnFamily.size());
			columnFamily.forEach(cf -> {
				cfDesc.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build());
			});
			// 表 table
			TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
					.setColumnFamilies(cfDesc).build();
			if (admin.tableExists(TableName.valueOf(tableName))) {
				log.debug("table Exists!");
			} else {
				admin.createTable(tableDesc);
				log.debug("create table Success!");
			}
		} catch (IOException e) {
			log.error(MessageFormat.format("创建表{0}失败", tableName), e);
			return false;
		} finally {
			close(admin, null, null);
		}
		return true;
	}*/

	/**
	 * 查询所有表的表名
	 */
	/*public List<String> getAllTableNames() {
		List<String> result = new ArrayList<>();
		try {
			TableName[] tableNames = admin.listTableNames();
			for (TableName tableName : tableNames) {
				result.add(tableName.getNameAsString());
			}
		} catch (IOException e) {
			log.error("获取所有表的表名失败", e);
		} finally {
			close(admin, null, null);
		}
		return result;
	}*/

	/**
	 * 遍历查询指定表中的所有数据
	 */
	/*public Map<String, Map<String, String>> getResultScanner(String tableName) {
		Scan scan = new Scan();
		return this.queryData(tableName, scan);
	}*/

	/**
	 * 通过表名及过滤条件查询数据
	 */
	/*private Map<String, Map<String, String>> queryData(String tableName, Scan scan) {
		// <rowKey,对应的行数据>
		Map<String, Map<String, String>> result = new HashMap<>();
		ResultScanner rs = null;
		// 获取表
		Table table = null;
		try {
			table = getTable(tableName);
			rs = table.getScanner(scan);
			for (Result r : rs) {
				// 每一行数据
				Map<String, String> columnMap = new HashMap<>();
				String rowKey = null;
				// 行键，列族和列限定符一起确定一个单元（Cell）
				for (Cell cell : r.listCells()) {
					if (rowKey == null) {
						rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
					}
					columnMap.put(
							// 列限定符
							Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
									cell.getQualifierLength()),
							// 列族
							Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
				}
				if (rowKey != null) {
					result.put(rowKey, columnMap);
				}
			}
		} catch (IOException e) {
			log.error(MessageFormat.format("遍历查询指定表中的所有数据失败,tableName:{0}", tableName), e);
		} finally {
			close(null, rs, table);
		}
		return result;
	}*/

	/**
	 * 为表添加或者更新数据
	 */
	/*public void putData(String tableName, String rowKey, String familyName, String[] columns, String[] values) {
		Table table = null;
		try {
			table = getTable(tableName);
			putData(table, rowKey, tableName, familyName, columns, values);
		} catch (Exception e) {
			log.error(MessageFormat.format("为表添加 or 更新数据失败,tableName:{0},rowKey:{1},familyName:{2}", tableName, rowKey,
					familyName), e);
		} finally {
			close(null, null, table);
		}
	}*/

	/*private void putData(Table table, String rowKey, String tableName, String familyName, String[] columns,
			String[] values) {
		try {
			// 设置rowkey
			Put put = new Put(Bytes.toBytes(rowKey));
			if (columns != null && values != null && columns.length == values.length) {
				for (int i = 0; i < columns.length; i++) {
					if (columns[i] != null && values[i] != null) {
						put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columns[i]), Bytes.toBytes(values[i]));
					} else {
						throw new NullPointerException(
								MessageFormat.format("列名和列数据都不能为空,column:{0},value:{1}", columns[i], values[i]));
					}
				}
			}
			table.put(put);
			log.debug("putData add or update data Success,rowKey:" + rowKey);
			table.close();
		} catch (Exception e) {
			log.error(MessageFormat.format("为表添加 or 更新数据失败,tableName:{0},rowKey:{1},familyName:{2}", tableName, rowKey,
					familyName), e);
		}
	}*/

	/**
	 * 根据表名获取table
	 */
	/*private Table getTable(String tableName) throws IOException {
		return connection.getTable(TableName.valueOf(tableName));
	}*/

	/**
	 * 关闭流
	 */
	/*private void close(Admin admin, ResultScanner rs, Table table) {
		if (admin != null) {
			try {
				admin.close();
			} catch (IOException e) {
				log.error("关闭Admin失败", e);
			}
			if (rs != null) {
				rs.close();
			}
			if (table != null) {
				rs.close();
			}
			if (table != null) {
				try {
					table.close();
				} catch (IOException e) {
					log.error("关闭Table失败", e);
				}
			}
		}
	}*/
}
