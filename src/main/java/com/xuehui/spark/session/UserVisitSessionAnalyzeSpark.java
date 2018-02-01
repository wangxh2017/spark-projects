package com.xuehui.spark.session;

import com.xuehui.conf.ConfigurationManager;
import com.xuehui.constants.CommonConstants;
import com.xuehui.untils.MockData;
import com.xuehui.untils.ParamUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2018/2/1.
 */
public class UserVisitSessionAnalyzeSpark {

    public static void main(String[] args) {
        //构建spark上下文
        SparkConf conf = new SparkConf()
                .setAppName("UserVisitSessionAnalyzeSpark")
                .setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sqlContext = getSQLContext(jsc);
        //生成模拟数据
        createMeta(jsc, sqlContext);
        //获取taskid
        Long taskId = ParamUtils.getTaskIdFromArgs(args);

        //关闭资源
        jsc.close();
    }

    /**
     * 获取SQLContext
     * @param jsc
     * @return
     */
    private static SQLContext getSQLContext(JavaSparkContext jsc){
        Boolean sparkLocalMode = ConfigurationManager.getBoolean(CommonConstants.SPARK_LOCAL_MODE);
        if(sparkLocalMode){
            return new SQLContext(jsc);
        }else{
            return new HiveContext(jsc.sc());
        }
    }

    private static  void createMeta(JavaSparkContext jsc, SQLContext sqlContext){
        Boolean sparkLocalMode = ConfigurationManager.getBoolean(CommonConstants.SPARK_LOCAL_MODE);
        if(sparkLocalMode){
            MockData.mock(jsc, sqlContext);
        }
    }



}
