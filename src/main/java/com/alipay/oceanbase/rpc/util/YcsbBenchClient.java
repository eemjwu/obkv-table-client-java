/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.sql.Timestamp;

import com.yahoo.ycsb.*;

import java.util.*;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;

public class YcsbBenchClient extends DB {
    public static final String OB_TABLE_CLIENT_FULL_USER_NAME          = "obkv.fullUserName";
    public static final String OB_TABLE_CLIENT_PARAM_URL               = "obkv.paramURL";
    public static final String OB_TABLE_CLIENT_PASSWORD                = "obkv.password";
    public static final String OB_TABLE_CLIENT_PROXY_SYS_USER_NAME     = "obkv.proxySysUserName";
    public static final String OB_TABLE_CLIENT_PROXY_SYS_USER_PASSWORD = "obkv.proxySysUserPassword";
    public static final String OB_TABLE_CLIENT_ROW_KEY                 = "obkv.rowKey";
    public static final String OB_TABLE_CLIENT_TABLE_NAME              = "obkv.tableName";
    public static final String OB_TABLE_CLIENT_CONN_TIMEOUT            = "obkv.connTimeOut";
    public static final String OB_TABLE_CLIENT_SOCK_TIMEOUT            = "obkv.sockTimeOut";
    public static final String OB_TABLE_CLIENT_OPERATION_TIMEOUT       = "obkv.operationTimeOut";
    public static final String OB_TABLE_CLIENT_RS_CONN_TIMEOUT         = "obkv.rsConnTimeOut";
    public static final String OB_TABLE_CLIENT_RS_READ_TIMEOUT         = "obkv.rsReadTimeOut";
    public static final String OB_TABLE_CLIENT_ODP_PORT                = "obkv.odpPort";
    public static final String OB_TABLE_CLIENT_IS_ODP_MODE             = "obkv.isOdpMode";
    public static final String OB_TABLE_CLIENT_DATABASE_NAME           = "obkv.databaseName";
    public static final String OB_TABLE_CLIENT_USE_INSERT_UP           = "obkv.useInsertUp";
    public static final String OB_TABLE_CLIENT_EXECUTE_RETRY_TIMES     = "obkv.executeRetryTimes";
    public static final String OB_TABLE_CLIENT_EXECUTE_MAX_WAIT        = "obkv.executeRetryTimes";
    public static final String OB_TABLE_CLIENT_USE_ROW_KEY_PREFIX      = "obkv.useRowKeyPrefix";
    public static final String OB_TABLE_CLIENT_CONN_POOL_SIZE          = "obkv.connPoolSize";
    public static final String OB_TABLE_CLIENT_LOGGING_PATH            = "obkv.loggingPath";
    public static final String OB_TABLE_CLIENT_LOGGING_LEVEL           = "obkv.loggingLevel";
    public static final String OB_TABLE_BATCH_SIZE                     = "obkv.batch_size";
    public static final String OB_TABLE_INSERT_PATITION                = "obkv.insert_partition";
    public static final String OB_TABLE_READ_FILED_SIZE                = "obkv.read_field_size";
    public static final String OB_TABLE_IS_TEST_300COL                 = "obkv.is_test_300col";

    // odp mode
    public boolean             isOdpMode                               = true;
    public int                 odpPort;
    public String              databaseName;

    ObTableClient              obTableClient;
    String[]                   fields                                  = new String[] { "FIELD0",
            "FIELD1", "FIELD2", "FIELD3", "FIELD4", "FIELD5", "FIELD6", "FIELD7", "FIELD8",
            "FIELD9"                                                  };

    String[]                   col_names                               = new String[] { "c_786",
            "c_788", "c_784", "c_701", "c_60", "c_80", "c_617", "c_638", "c_172", "c_173", "c_882",
            "c_615", "c_537", "c_625", "c_883", "c_923", "c_707", "c_61", "c_62", "c_70", "c_71",
            "c_72", "c_88", "c_89", "c_91", "c_92", "c_94", "c_591", "c_117", "c_174", "c_175",
            "c_616", "c_618", "c_619", "c_623", "c_624", "c_626", "c_627", "c_628", "c_886",
            "c_850", "c_851", "c_816", "c_817", "c_818", "c_848", "c_821", "c_849", "c_905",
            "c_578", "c_918", "c_737", "c_212", "c_575", "c_583", "c_358", "c_535", "c_432",
            "c_373", "c_368", "c_1257", "c_686", "c_336", "c_770", "c_584", "c_132", "c_140",
            "c_82", "c_590", "c_793", "c_69", "c_102", "c_473", "c_519", "c_520", "c_204", "c_654",
            "c_95", "c_122", "c_107", "c_213", "c_265", "c_123", "c_244", "c_245", "c_246",
            "c_247", "c_511", "c_268", "c_895", "c_477", "c_133", "c_141", "c_86", "c_542",
            "c_543", "c_858", "c_890", "c_939", "c_687", "c_116", "c_699", "c_684", "c_714",
            "c_83", "c_68", "c_1216", "c_667", "c_999", "c_921", "c_898", "c_593", "c_594",
            "c_1033", "c_857", "c_538", "c_539", "c_856", "c_655", "c_861", "c_808", "c_527",
            "c_1057", "c_1058", "c_1582", "c_640", "c_641", "c_642", "c_884", "c_620", "c_621",
            "c_666", "c_1292", "c_1000", "c_90", "c_99", "c_100", "c_103", "c_104", "c_1302",
            "c_1911", "c_922", "c_946", "c_762", "c_750", "c_431", "c_938", "c_1304", "c_1285",
            "c_1290", "c_125", "c_663", "c_671", "c_81", "c_337", "c_109", "c_400", "c_340",
            "c_110", "c_834", "c_471", "c_820", "c_822", "c_458", "c_460", "c_459", "c_461",
            "c_688", "c_678", "c_472", "c_831", "c_833", "c_829", "c_785", "c_977", "c_787",
            "c_715", "c_716", "c_717", "c_718", "c_659", "c_660", "c_512", "c_528", "c_544",
            "c_838", "c_839", "c_841", "c_859", "c_860", "c_779", "c_780", "c_891", "c_842",
            "c_847", "c_835", "c_837", "c_827", "c_789", "c_790", "c_523", "c_447", "c_690",
            "c_522", "c_524", "c_867", "c_677", "c_576", "c_220", "c_933", "c_709", "c_704",
            "c_366", "c_664", "c_1004", "c_768", "c_119", "c_706", "c_719", "c_760", "c_587",
            "c_588", "c_670", "c_669", "c_668", "c_658", "c_796", "c_800", "c_702", "c_763",
            "c_811", "c_693", "c_742", "c_743", "c_744", "c_711", "c_518", "c_602", "c_606",
            "c_348", "c_349", "c_120", "c_1481", "c_1482", "c_359", "c_360", "c_361", "c_362",
            "c_365", "c_480", "c_481", "c_482", "c_483", "c_507", "c_656", "c_596", "c_551",
            "c_552", "c_196", "c_650", "c_651", "c_488", "c_487", "c_1256", "c_1258", "c_1291",
            "c_878", "c_879", "c_880", "c_967", "c_968", "c_901", "c_1524", "c_1525", "c_1526",
            "c_1527", "c_881", "c_75", "c_836", "c_776", "c_741", "c_755", "c_995", "c_832",
            "c_222", "c_221", "c_1223", "c_1224", "c_1034", "c_1035", "c_521", "c_764", "c_705",
            "c_534", "c_894", "c_774", "c_962", "c_754", "c_139", "c_145", "c_965", "c_959",
            "c_691", "c_697", "c_694", "c_695", "c_679", "c_563", "c_344", "c_345", "c_148",
            "c_149", "c_548", "c_720", "c_852", "c_124", "c_386", "c_614", "c_1175", "c_2016",
            "c_1672", "c_44", "c_41", "c_2024", "c_2015", "c_1288", "c_484", "c_426", "c_1679",
            "c_42"                                                    };

    String                     rowKey;                                                               // just for print when execute failed
    String                     tableName;                                                            // just for print when execute failed
    String                     rowKeyPostfix;                                                        // currenct process name
    boolean                    useInsertUp;
    boolean                    useRowKeyPrefix                         = false;
    int                        batchSize;
    int                        patition_key;

    int                        read_field_size;

    boolean                    is_test_300col                          = false;

    /**
     * Init.
     */
    public void init() throws DBException {
        Properties props = getProperties();
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        this.rowKeyPostfix = runtimeMXBean.getName();

        String useInsertUpStr = props.getProperty(OB_TABLE_CLIENT_USE_INSERT_UP);
        boolean useInsertUp = false;
        if (useInsertUpStr != null) {
            useInsertUp = Boolean.parseBoolean(useInsertUpStr);
        }
        this.useInsertUp = useInsertUp;

        String useRowKeyPrefixStr = props.getProperty(OB_TABLE_CLIENT_USE_ROW_KEY_PREFIX);
        boolean useRowKeyPrefix = false;
        if (useRowKeyPrefixStr != null) {
            useRowKeyPrefix = Boolean.parseBoolean(useRowKeyPrefixStr);
        }
        this.useRowKeyPrefix = useRowKeyPrefix;

        String fullUserName = props.getProperty(OB_TABLE_CLIENT_FULL_USER_NAME);
        if (fullUserName == null) {
            throw new DBException("fullUserName can not be null");
        }

        String paramURL = props.getProperty(OB_TABLE_CLIENT_PARAM_URL);
        if (paramURL == null) {
            throw new DBException("paramURL can not be null");
        }

        String password = props.getProperty(OB_TABLE_CLIENT_PASSWORD);
        if (password == null) {
            password = "";
        }

        String loggingPath = props.getProperty(OB_TABLE_CLIENT_LOGGING_PATH);
        if (loggingPath != null) {
            System.setProperty("logging.path", loggingPath);
        } else {
            System.setProperty("logging.path", System.getProperty("user.dir") + "/logs");
        }

        String loggingLevel = props.getProperty(OB_TABLE_CLIENT_LOGGING_LEVEL);
        if (loggingLevel != null) {
            System.setProperty("logging.level", loggingLevel);
        } else {
            System.setProperty("logging.level", "WARN");
        }

        final ObTableClient obTableClient = new ObTableClient();
        String isOdpModeStr = props.getProperty(OB_TABLE_CLIENT_IS_ODP_MODE);
        if (isOdpModeStr != null) {
            isOdpMode = Boolean.parseBoolean(isOdpModeStr);
        }
        if (!isOdpMode) {
            // in client direct connection mode, need proxy user and rowKey element
            String proxySysUserName = props.getProperty(OB_TABLE_CLIENT_PROXY_SYS_USER_NAME);
            if (proxySysUserName == null) {
                throw new DBException("proxySysUserName can not be null");
            }

            String proxySysUserPassword = props
                .getProperty(OB_TABLE_CLIENT_PROXY_SYS_USER_PASSWORD);
            if (proxySysUserPassword == null) {
                proxySysUserPassword = "";
            }

            String rowKey = props.getProperty(OB_TABLE_CLIENT_ROW_KEY);
            if (rowKey == null) {
                throw new DBException("rowKey can not be null");
            }
            this.rowKey = rowKey;

            String tableName = props.getProperty(OB_TABLE_CLIENT_TABLE_NAME);
            if (tableName == null) {
                throw new DBException("tableName can not be null");
            }
            this.tableName = tableName;

            obTableClient.setFullUserName(fullUserName);
            obTableClient.setParamURL(paramURL);
            obTableClient.setPassword(password);
            obTableClient.setSysUserName(proxySysUserName);
            obTableClient.setSysPassword(proxySysUserPassword);
        } else {
            obTableClient.setOdpMode(isOdpMode);
            obTableClient.setFullUserName(fullUserName);
            obTableClient.setOdpAddr(paramURL);
            obTableClient.setPassword(password);
            String odpPortStr = props.getProperty(OB_TABLE_CLIENT_ODP_PORT);
            if (odpPortStr == null) {
                throw new DBException("odp port can not be null in odp mode");
            }
            odpPort = Integer.parseInt(odpPortStr);
            obTableClient.setOdpPort(odpPort);
            databaseName = props.getProperty(OB_TABLE_CLIENT_DATABASE_NAME);
            if (databaseName == null) {
                throw new DBException("database can not be null in odp mode");
            }
            obTableClient.setDatabase(databaseName);
        }

        try {
            String connTimeOut = props.getProperty(OB_TABLE_CLIENT_CONN_TIMEOUT);
            if (connTimeOut != null) {
                obTableClient.addProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), connTimeOut);
                obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), connTimeOut);
            } else {
                obTableClient.addProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), "30000");
            }

            String sockTimeOut = props.getProperty(OB_TABLE_CLIENT_SOCK_TIMEOUT);
            if (sockTimeOut != null) {
                obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), sockTimeOut);
            } else {
                obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "35000");
            }

            String operationTimeOut = props.getProperty(OB_TABLE_CLIENT_OPERATION_TIMEOUT);
            if (operationTimeOut != null) {
                obTableClient
                    .addProperty(Property.RPC_OPERATION_TIMEOUT.getKey(), operationTimeOut);
            } else {
                obTableClient.addProperty(Property.RPC_OPERATION_TIMEOUT.getKey(), "25000");
            }

            String rsConnTimeOut = props.getProperty(OB_TABLE_CLIENT_RS_CONN_TIMEOUT);
            if (rsConnTimeOut != null) {
                obTableClient.addProperty(Property.RS_LIST_ACQUIRE_CONNECT_TIMEOUT.getKey(),
                    rsConnTimeOut);
            } else {
                obTableClient
                    .addProperty(Property.RS_LIST_ACQUIRE_CONNECT_TIMEOUT.getKey(), "2000");
            }

            String rsReadTimeOut = props.getProperty(OB_TABLE_CLIENT_RS_READ_TIMEOUT);
            if (operationTimeOut != null) {
                obTableClient.addProperty(Property.RS_LIST_ACQUIRE_READ_TIMEOUT.getKey(),
                    rsReadTimeOut);
            } else {
                obTableClient.addProperty(Property.RS_LIST_ACQUIRE_READ_TIMEOUT.getKey(), "10000");
            }

            String executeRetryTimes = props.getProperty(OB_TABLE_CLIENT_EXECUTE_RETRY_TIMES);
            if (executeRetryTimes == null) {
                obTableClient.addProperty(Property.RUNTIME_RETRY_TIMES.getKey(), "3");
            } else {
                obTableClient.addProperty(Property.RUNTIME_RETRY_TIMES.getKey(), executeRetryTimes);
            }

            String executeMaxWait = props.getProperty(OB_TABLE_CLIENT_EXECUTE_MAX_WAIT);
            if (executeMaxWait == null) {
                obTableClient.addProperty(Property.RUNTIME_MAX_WAIT.getKey(), "30000");
            } else {
                obTableClient.addProperty(Property.RUNTIME_MAX_WAIT.getKey(), executeMaxWait);
            }

            String connPoolSizeStr = props.getProperty(OB_TABLE_CLIENT_CONN_POOL_SIZE);
            if (connPoolSizeStr == null) {
                System.out.println("use default pool size: 1");
            } else {
                System.out.println("current pool size is: " + connPoolSizeStr);
                obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(),
                    connPoolSizeStr);
            }

            this.batchSize = 1;
            String batchSizeStr = props.getProperty(OB_TABLE_BATCH_SIZE);
            if (batchSizeStr == null) {
                System.out.println("use default batchSize: 1");
            } else {
                System.out.println("batchSize is: " + batchSizeStr);
                this.batchSize = Integer.parseInt(batchSizeStr);
            }
            assert (this.batchSize > 0);

            this.patition_key = -1;
            String partitionStr = props.getProperty(OB_TABLE_INSERT_PATITION);
            if (partitionStr == null) {
                System.out.println("use default partitionStr: -1");
            } else {
                System.out.println("partitionStr is: " + partitionStr);
                this.patition_key = Integer.parseInt(partitionStr);
            }

            this.read_field_size = -1;
            String readFiledSizeStr = props.getProperty(OB_TABLE_READ_FILED_SIZE);
            if (readFiledSizeStr == null) {
                System.out.println("use default readFiledSizeStr: -1 means read all filed");
            } else {
                System.out.println("readFiledSizeStr is: " + readFiledSizeStr);
                this.read_field_size = Integer.parseInt(readFiledSizeStr);
            }

            String isTest300Col = props.getProperty(OB_TABLE_IS_TEST_300COL);
            if (isTest300Col != null) {
                this.is_test_300col = Boolean.parseBoolean(isTest300Col);
            }

            obTableClient.init();
            this.obTableClient = obTableClient;
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    /**
     * Read.
     */
    @Override
    public Status read(String table, String key, Set<String> fields,
                       HashMap<String, ByteIterator> result) {

        try {
            // 测试读 300 列
            if (this.is_test_300col && this.batchSize != 1) {
                return readBatch300Col(table, key);
            } else if (this.is_test_300col) {
                return read300Col(table, key);
            } else if (this.batchSize != 1) {
                return readBatch10Col(table, key);
            }

            // 默认读全列
            List<String> fs = new ArrayList<String>(this.fields.length);
            if (read_field_size < 0 || read_field_size >= this.fields.length) {
                Collections.addAll(fs, this.fields);
            } else {
                Collections.addAll(fs, Arrays.copyOfRange(this.fields, 0, read_field_size));
            }

            int patition_key = this.patition_key;
            if (patition_key == -1) {
                Random random = new Random();
                patition_key = random.nextInt(4) + 1;
            }

            obTableClient.addRowKeyElement(table, new String[] { "id", "ycsb_key" });

            Map<String, Object> res = obTableClient.get(table, new Object[] { patition_key, key },
                fs.toArray(new String[0]));

            return res.isEmpty() ? Status.NOT_FOUND : Status.OK;
        } catch (Exception e) {
            printException(e);
        }
        return Status.ERROR;
    }

    public Status read300Col(String table, String key) {

        try {
            // 默认读全列
            List<String> fs = new ArrayList<String>(this.col_names.length);
            if (read_field_size < 0 || read_field_size >= this.col_names.length) {
                Collections.addAll(fs, this.col_names);
            } else {
                Collections.addAll(fs, Arrays.copyOfRange(this.col_names, 0, read_field_size));
            }

            int patition_key = this.patition_key;
            if (patition_key == -1) {
                Random random = new Random();
                patition_key = random.nextInt(4) + 1;
            }

            obTableClient.addRowKeyElement(table, new String[] { "id", "ycsb_key" });

            Map<String, Object> res = obTableClient.get(table, new Object[] { patition_key, key },
                fs.toArray(new String[0]));

            return res.isEmpty() ? Status.NOT_FOUND : Status.OK;
        } catch (Exception e) {
            printException(e);
        }
        return Status.ERROR;
    }

    public Status readBatch10Col(String table, String key) {
        try {
            // 默认读全列
            List<String> fs = new ArrayList<String>(this.fields.length);
            if (read_field_size < 0 || read_field_size >= this.fields.length) {
                Collections.addAll(fs, this.fields);
            } else {
                Collections.addAll(fs, Arrays.copyOfRange(this.fields, 0, read_field_size));
            }

            int patition_key = this.patition_key;
            if (patition_key == -1) {
                Random random = new Random();
                patition_key = random.nextInt(4);
            }

            obTableClient.addRowKeyElement(table, new String[] { "id", "ycsb_key" });

            // 当设置setSamePropertiesNames(true)时，表中所有的列都必须填充值
            BatchOperation batchOperation = obTableClient.batchOperation(tableName).setIsAtomic(
                true);
            obTableClient.addRowKeyElement(table, new String[] { "id", "ycsb_key" });

            for (long i = 0; i < batchSize; i++) {
                // 清除毫秒部分，仅保留到秒
                Row rowKey = new Row(colVal("id", patition_key), colVal("ycsb_key",
                    key + String.valueOf(i)));

                TableQuery query = query().setRowKey(rowKey).select(fs.toArray(new String[0]));

                batchOperation.addOperation(query);
            }
            batchOperation.setIsAtomic(true).execute();

            return Status.OK;
        } catch (Exception e) {
            printException(e);
        }
        return Status.ERROR;
    }

    public Status readBatch300Col(String table, String key) {

        try {
            // 默认读全列
            List<String> fs = new ArrayList<String>(this.col_names.length);
            if (read_field_size < 0 || read_field_size >= this.col_names.length) {
                Collections.addAll(fs, this.col_names);
            } else {
                Collections.addAll(fs, Arrays.copyOfRange(this.col_names, 0, read_field_size));
            }

            int patition_key = this.patition_key;
            if (patition_key == -1) {
                Random random = new Random();
                patition_key = random.nextInt(4);
            }

            obTableClient.addRowKeyElement(table, new String[] { "id", "ycsb_key" });

            // 当设置setSamePropertiesNames(true)时，表中所有的列都必须填充值
            BatchOperation batchOperation = obTableClient.batchOperation(tableName).setIsAtomic(
                true);
            obTableClient.addRowKeyElement(table, new String[] { "id", "ycsb_key" });

            for (long i = 0; i < batchSize; i++) {
                // 清除毫秒部分，仅保留到秒
                Row rowKey = new Row(colVal("id", patition_key), colVal("ycsb_key",
                    key + String.valueOf(i)));

                TableQuery query = query().setRowKey(rowKey).select(fs.toArray(new String[0]));

                batchOperation.addOperation(query);
            }
            batchOperation.setIsAtomic(true).execute();

            return Status.OK;
        } catch (Exception e) {
            printException(e);
        }
        return Status.ERROR;
    }

    /**
     * Scan.
     */
    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {

        try {
            String[] fs = this.fields;
            if (fields != null) {
                fs = fields.toArray(new String[] {});
            }
            TableQuery tableQuery = obTableClient.query(table);
            tableQuery.limit(recordcount);
            tableQuery.select(fs);
            tableQuery.addScanRange(startkey, ObObj.getMax());
            QueryResultSet set = tableQuery.execute();

            while (set.next()) {
                HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
                Map<String, Object> row = set.getRow();
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    map.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
                }
                result.add(map);
            }

            return result.isEmpty() ? Status.ERROR : Status.OK;
        } catch (Exception e) {
            printException(e);
        }
        return null;
    }

    /**
     * Update.
     */
    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        Map<String, String> vs = StringByteIterator.getStringMap(values);
        String[] fields = vs.keySet().toArray(new String[] {});
        String[] fieldValues = vs.values().toArray(new String[] {});
        try {
            obTableClient.update(table, key, fields, fieldValues);
            return Status.OK;
        } catch (Exception e) {
            printException(e);
        }
        return Status.ERROR;
    }

    /**
     * Insert.
     */
    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            if (this.is_test_300col && this.batchSize != 1) {
                return multiInsert300Col(table, key);
            } else if (this.is_test_300col) {
                return insert300Col(table, key);
            } else if (this.batchSize != 1) {
                return multiInsert10Col(table, key, values);
            }

            Map<String, String> vs = StringByteIterator.getStringMap(values);
            String[] fields = vs.keySet().toArray(new String[] {});
            String[] fieldValues = vs.values().toArray(new String[] {});

            int patition_key = this.patition_key;
            if (patition_key == -1) {
                Random random = new Random();
                patition_key = random.nextInt(4);
            }

            obTableClient.addRowKeyElement(table, new String[] { "id", "ycsb_key" });

            if (useInsertUp) {
                obTableClient.insertOrUpdate(table, new Object[] { patition_key, key }, fields,
                    fieldValues);
            } else {
                obTableClient.insert(table, new Object[] { this.patition_key, key }, fields,
                    fieldValues);
            }

            return Status.OK;
        } catch (Exception e) {
            printException(e);
        }
        return Status.ERROR;
    }

    public Status multiInsert10Col(String tableName, String key,
                                   HashMap<String, ByteIterator> values) throws Exception {
        Map<String, String> vs = StringByteIterator.getStringMap(values);
        ColumnValue[] columnValuesArray = new ColumnValue[values.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : vs.entrySet()) {
            columnValuesArray[i] = new ColumnValue(entry.getKey(), entry.getValue());
        }

        int patition_key = this.patition_key;
        if (patition_key == -1) {
            Random random = new Random();
            // 1 - 4
            patition_key = random.nextInt(4) + 1;
        }

        BatchOperation batchOperation = obTableClient.batchOperation(tableName).setIsAtomic(true);
        obTableClient.addRowKeyElement(tableName, new String[] { "id", "ycsb_key" });

        for (long idx = 0; idx < batchSize; idx++) {
            Row rowKey = new Row(colVal("id", patition_key), colVal("ycsb_key",
                key + String.valueOf(idx)));

            InsertOrUpdate putOp = insertOrUpdate().setRowKey(rowKey);
            putOp.addMutateColVal(columnValuesArray);

            batchOperation.addOperation(putOp);
        }
        BatchOperationResult result = batchOperation.setIsAtomic(true).execute();

        return Status.OK;
    }

    public Status insert300Col(String table, String key) throws Exception {


        int patition_key = this.patition_key;
        if (patition_key == -1) {
            Random random = new Random();
            patition_key = random.nextInt(4);
        }

        //清除毫秒部分，仅保留到秒
        long timeInSeconds = (System.currentTimeMillis() / 1000) * 1000;
        Timestamp ts = new Timestamp(timeInSeconds);
        java.util.Date date = new java.sql.Date(timeInSeconds);


        List<ColumnValue> columnValueList = new ArrayList<>(col_names.length + 6);
        columnValueList.add(new ColumnValue("b_1", "2.1.0.2"));
        columnValueList.add(new ColumnValue("b_2", "V_1_8_DA_05"));
        columnValueList.add(new ColumnValue("b_3", "periodical_journey_update"));
        columnValueList.add(new ColumnValue("t_2", ts));
        columnValueList.add(new ColumnValue("t_3", ts));
        columnValueList.add(new ColumnValue("t_4", ts));

        int inx = 0;
        long number = 1697135265424L;
        for (; inx < col_names.length / 2; inx++) {
            columnValueList.add(new ColumnValue(col_names[inx], number));
        }
        for (; inx < col_names.length; inx++) {
            columnValueList.add(new ColumnValue(col_names[inx], null));
        }

        Row rowKey = new Row(colVal("id", patition_key), colVal("ycsb_key",
                key));

        obTableClient.addRowKeyElement(table, new String[]{"id", "ycsb_key"});

        if (useInsertUp) {
            InsertOrUpdate putOp = obTableClient.insertOrUpdate(tableName).setRowKey(rowKey);
            putOp.addMutateColVal(columnValueList.toArray(new ColumnValue[0]));
            putOp.execute();
        } else {
            Insert putOp = obTableClient.insert(tableName).setRowKey(rowKey);
            putOp.addMutateColVal(columnValueList.toArray(new ColumnValue[0]));
            putOp.execute();
        }

        return Status.OK;
    }

    public Status multiInsert300Col(String tableName, String key) throws Exception {
        int patition_key = this.patition_key;
        if (patition_key == -1) {
            Random random = new Random();
            // 1 - 4
            patition_key = random.nextInt(4) + 1;
        }

        // 构造写入列数据
        //清除毫秒部分，仅保留到秒
        long timeInSeconds = (System.currentTimeMillis() / 1000) * 1000;
        Timestamp ts = new Timestamp(timeInSeconds);
        java.util.Date date = new java.sql.Date(timeInSeconds);


        List<ColumnValue> columnValueList = new ArrayList<>(col_names.length + 6);
        columnValueList.add(new ColumnValue("b_1", "2.1.0.2"));
        columnValueList.add(new ColumnValue("b_2", "V_1_8_DA_05"));
        columnValueList.add(new ColumnValue("b_3", "periodical_journey_update"));
        columnValueList.add(new ColumnValue("t_2", ts));
        columnValueList.add(new ColumnValue("t_3", ts));
        columnValueList.add(new ColumnValue("t_4", ts));

        int inx = 0;
        long number = 1697135265424L;
        for (; inx < col_names.length / 2; inx++) {
            columnValueList.add(new ColumnValue(col_names[inx], number));
        }
        for (; inx < col_names.length; inx++) {
            columnValueList.add(new ColumnValue(col_names[inx], null));
        }
        ColumnValue[] columnValueArray =  columnValueList.toArray(new ColumnValue[0]);


        BatchOperation batchOperation = obTableClient.batchOperation(tableName).setIsAtomic(true);
        obTableClient.addRowKeyElement(tableName, new String[]{"id", "ycsb_key"});

        for (long idx = 0; idx < batchSize; idx++) {
            Row rowKey = new Row(colVal("id", patition_key), colVal("ycsb_key",
                    key + String.valueOf(idx)));

            InsertOrUpdate putOp = insertOrUpdate().setRowKey(rowKey);
            putOp.addMutateColVal(columnValueArray);

            batchOperation.addOperation(putOp);
        }
        BatchOperationResult result = batchOperation.setIsAtomic(true).execute();

        return Status.OK;
    }

    //    public Status BatchPut300Row(String tableName, String key) throws Exception {
    //        // 创建一个随机数生成器对象
    //        Random random = new Random();
    //
    //        // 生成0到4之间的随机数
    //        long patition_key = random.nextInt(4);
    //
    //        // 当设置setSamePropertiesNames(true)时，表中所有的列都必须填充值
    //        BatchOperation batchOperation = obTableClient.batchOperation(tableName).setIsAtomic(true)
    //                .setSamePropertiesNames(true);
    //
    //        for (long i = 0; i < batchSize; i++) {
    //            // 清除毫秒部分，仅保留到秒
    //            long timeInSeconds = (System.currentTimeMillis() / 1000) * 1000;
    //            Timestamp ts = new Timestamp(timeInSeconds);
    //            java.util.Date date = new java.sql.Date(timeInSeconds);
    //            Row rowKey = new Row(colVal("t_1", patition_key), colVal("id", key + String.valueOf(i)));
    //
    //            Put putOp = put().setRowKey(rowKey)
    //                    .addMutateColVal(colVal("b_1", "2.1.0.2"))
    //                    // `b_1` varchar(32)
    //                    .addMutateColVal(colVal("b_2", "V_1_8_DA_05"))
    //                    .addMutateColVal(colVal("b_3", "periodical_journey_update"))
    //                    .addMutateColVal(colVal("t_2", ts)) // `t_2` timestamp(3)
    //                    .addMutateColVal(colVal("t_3", ts)) // `t_3` timestamp(3)
    //                    .addMutateColVal(colVal("t_4", ts)); // `c_1` bigint(20)
    //            int inx = 0;
    //            for (; inx < col_names.length / 2; inx++) {
    //                long number = 1697135265424L;
    //                putOp.addMutateColVal(colVal(col_names[inx], number));
    //            }
    //            for (; inx < col_names.length; inx++) {
    //                putOp.addMutateColVal(colVal(col_names[inx], null));
    //            }
    //
    //            batchOperation.addOperation(putOp);
    //        }
    //        BatchOperationResult result = batchOperation.setIsAtomic(true).execute();
    //
    //        return Status.OK;
    //    }

    /**
     * Delete.
     */
    @Override
    public Status delete(String table, String key) {
        try {
            return obTableClient.delete(table, key) == 0 ? Status.ERROR : Status.OK;
        } catch (Exception e) {
            printException(e);
        }

        return Status.ERROR;
    }

    public void printException(Exception e) {
        e.printStackTrace();
        if (!isOdpMode) {
            System.err.println("fullUserName:" + obTableClient.getFullUserName() + " url :"
                               + obTableClient.getParamURL());
        } else {
            //            System.err.println("fullUserName:" + obTableClient.getFullUserName() + " ip :"
            //                               + obTableClient.getOdpAddr() + " port: "
            //                               + obTableClient.getOdpPort());
        }
    }
}
