package com.steer.mysql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

public class shell {
    /**
     * mysql 主机地址
     */
    private static final String HOST = "127.0.0.1";
    /**
     * mysql 用户名
     */
    private static final String USERNAME = "root";
    /**
     * mysql 密码
     */
    private static final String PASSWORD = "root";
    /**
     * mysql 数据库名
     */
    private static final String DATABASE = "smart_raw";
    /**
     * 导出sql文件的目标路径
     */
    private static final String OUTPUT_PATH = "/home/fangwk/Desktop/";
    private static final String PARAM = " -ct --skip-extended-insert --complete-insert --compact --default-character-set=utf8 ";

    public static void main(String[] args){
        /**
         * 导出equipment_type表
         */
        String[] equipmentIds = new String[]{"010","020"};
        exportEquipmentTypeSql(equipmentIds);

        String[] deviceModelIds = new String[]{"91","96"};
        /**
         * 导出device_models,device_protocols,device_model_checkpoints表
         */
        exportDeviceModelSql(deviceModelIds);
        exportDeviceProtocolSql(deviceModelIds);
        exportDeviceModelCheckpointSql(deviceModelIds);
        /**
         * 导出metric,event_types表
         * typeCode:  equipmentype id
         * startCode: 起始code（包含）
         * endCode: 结束code（包含）
         */
        String typeCode = "140";
        String startCode = "22";
        String endCode = "28";
        testExportMetricAndEventTypeSql(typeCode,startCode,endCode);

    }

    public static void exportEquipmentTypeSql(String[] ids){
        String conditions = buildInCondition("id",ids);
        executeCmd(buildCmd(TABLE_NAME.EQUIPMENT_TYPE.getName(),conditions));
    }

    public static void exportDeviceModelSql(String[] deviceModelIds){
        String conditions = buildInCondition("id",deviceModelIds);
        executeCmd(buildCmd(TABLE_NAME.DEVICE_MODEL.getName(),conditions));
    }

    public static void exportDeviceProtocolSql(String[] deviceModelIds){
        String conditions = buildInCondition("model_id",deviceModelIds);
        executeCmd(buildCmd(TABLE_NAME.DEVICE_PROTOCOL.getName(),conditions));
    }

    public static void testExportMetricAndEventTypeSql(String typeCode,String startCode,String endCode){

        String cmdDropTable = new StringBuilder().
                append(cmdMysqlPre.toString()).
                append(" \"DROP TABLE IF EXISTS ").append(TABLE_NAME.METRIC.getName()).append("\"").toString();

        executeCmd(cmdDropTable);

        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String cmdCreatTmpTable = new StringBuilder().
                append(cmdMysqlPre.toString()).
                append(" \"CREATE TABLE ").append(TABLE_NAME.METRIC.getName()).
                append(" SELECT id,category,code,measure_type,name,restore_acceptable_time,alarm_repeat_time,type_code,value_type,alarm_acceptable_time,default_event").
                append(" FROM ").append(TABLE_NAME.METRIC_EXPAND.getName()).append("\"").toString();

        executeCmd(cmdCreatTmpTable);

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        //metric
        String condition1 = String.format("type_code=%s AND CONVERT(code, UNSIGNED INTEGER) >= %s AND CONVERT(code, UNSIGNED INTEGER) <= %s",typeCode,startCode,endCode);

        executeCmd(buildCmd(TABLE_NAME.METRIC.getName(),condition1));

        //event_type
        String condition2 = String.format("LEFT(id,3)=%s AND CONVERT(SUBSTRING(id,4,4),UNSIGNED INTEGER) >= %s AND CONVERT(SUBSTRING(id,4,4), UNSIGNED INTEGER) <= %s",typeCode,startCode,endCode);

        executeCmd(buildCmd(TABLE_NAME.EVENT_TYPE.getName(),condition2));

        executeCmd(cmdDropTable);
    }

    public static void exportDeviceModelCheckpointSql(String[] deviceModelIds){
        String conditions = buildInCondition("model_id",deviceModelIds);
        executeCmd(buildCmd(TABLE_NAME.DEVICE_MODEL_CHECKPOINTS.getName(),conditions));
    }



    private static void executeCmd(String cmd){
        try {
            List<String> cmds = new LinkedList<String>();
            String os = System.getProperty("os.name").toLowerCase();
            if(os.startsWith("win")){
                cmds.add("cmd");
                cmds.add("/c");
            }else if(os.startsWith("linux")){
                cmds.add("sh");
                cmds.add("-c");
            }else{
                throw new Exception("未知操作系统："+os);
            }
            cmds.add(cmd);
            ProcessBuilder pb = new ProcessBuilder(cmds);
            //重定向到标准输出
            pb.redirectErrorStream(true);
            Process process = pb.start();
            process.waitFor();
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuffer sb = new StringBuffer();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            System.out.println("日志："+sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String buildInCondition(String columnName,String[] values){
        StringBuilder conditions = new StringBuilder().
                append(columnName).append(" in (");
        for (int i=0;i<values.length;i++){
            conditions.append(values[i]);
            if (i!=values.length-1){
                conditions.append(",");
            }
        }
        conditions.append(")");
        return conditions.toString();
    }


    private static String buildCmd(String tableName,String condition){
        StringBuilder cmd = new StringBuilder().
                append(cmdMysqlDumpPre.toString()).
                append(tableName).append(" ").
                append("--where=\"").append(condition).append("\"").
                append(" > ").
                append(OUTPUT_PATH).append(TABLE_NAME.getIdxByName(tableName)).append(".").append(tableName).append(".sql");
        System.out.println("文件输出路径："+OUTPUT_PATH+TABLE_NAME.getIdxByName(tableName)+"."+tableName+".sql");
        return cmd.toString();
    }

    /**
     * mysql 表名
     */
    private enum TABLE_NAME{
        EQUIPMENT_TYPE("equipment_types",1),
        DEVICE_MODEL("device_models",2),
        DEVICE_PROTOCOL("device_protocols",3),
        METRIC("metrics",4),
        EVENT_TYPE("event_types",5),
        DEVICE_MODEL_CHECKPOINTS("device_model_checkpoints",6),
        METRIC_EXPAND("metrics_expand",1000);
        private String name;
        private int idx;
        TABLE_NAME(String name,int idx){
            this.name = name;
            this.idx = idx;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getIdx() {
            return idx;
        }

        public void setIdx(int idx) {
            this.idx = idx;
        }

        public static int getIdxByName(String name){
            for(TABLE_NAME tableName: TABLE_NAME.values()){
                if (tableName.getName().equals(name)){
                    return tableName.getIdx();
                }
            }
            throw new IllegalArgumentException("不存在name为"+name+"的枚举");
        }

    }

    private static StringBuilder cmdMysqlDumpPre = new StringBuilder().
            append("mysqldump -h").append(HOST).
            append(" -u").append(USERNAME).
            append(" -p").append(PASSWORD).
            append(PARAM).
            append(DATABASE).append(" ");

    private static StringBuilder cmdMysqlPre = new StringBuilder().
            append("mysql -h").append(HOST).
            append(" -u").append(USERNAME).
            append(" -p").append(PASSWORD).
            append(" ").append(DATABASE).append(" -e");

}
