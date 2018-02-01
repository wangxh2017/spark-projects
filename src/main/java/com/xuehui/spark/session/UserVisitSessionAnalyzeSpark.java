package com.xuehui.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.xuehui.constants.CommonConstants;
import com.xuehui.mapper.TaskMapper;
import com.xuehui.pojo.Task;
import com.xuehui.untils.MockData;
import com.xuehui.untils.ParamUtils;
import com.xuehui.untils.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.stereotype.Service;
import scala.Serializable;
import scala.Tuple2;

import javax.annotation.Resource;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Administrator on 2018/2/1.
 */
@Service
public class UserVisitSessionAnalyzeSpark implements Serializable{
    @Resource transient
    public JavaSparkContext sc;
    @Resource transient
    public SQLContext sqlContext;
    @Resource transient
    private TaskMapper taskMapper;


    public void run(String[] args) {
        //生成模拟数据
        createMeta();
        //获取taskid
        Long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskMapper.findTaskByTaskId(taskId);
        JSONObject params = JSONObject.parseObject(task.getTaskParam());
        //获取指定时间内的用户行为数据，并封装到RDD中
        JavaRDD<Row> actionRDD = getActionRDD(params);
        //将需要的信息都聚合起来
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = sessionid2ActionsRDD(actionRDD);
        //根绝sessioni的进行过滤
    }

    /**
     * 获取SQLContext
     * @param jsc
     * @return
     */
    private SQLContext getSQLContext(JavaSparkContext jsc){
//        Boolean sparkLocalMode = ConfigurationManager.getBoolean(CommonConstants.SPARK_LOCAL_MODE);
//        if(sparkLocalMode){
//            return this.sqlContext;
//        }else{
//            return new HiveContext(jsc.sc());
//        }
        return sqlContext;
    }

    private  void createMeta(){
//        Boolean sparkLocalMode = ConfigurationManager.getBoolean(CommonConstants.SPARK_LOCAL_MODE);
//        if(sparkLocalMode){
//            MockData.mock(jsc, sqlContext);
//        }
        MockData.mock(sc, sqlContext);
    }

    /**
     * 获取指定时间内的用户行为数据，并封装到JavaRDD中
     * @param params
     * @return
     */
    private JavaRDD<Row> getActionRDD(JSONObject params){
        String startTime = ParamUtils.getParam(params, CommonConstants.SPARK_PARAM_START_TIME);
        String endTime = ParamUtils.getParam(params, CommonConstants.SPARK_PARAM_END_TIME);
        String sql = "" +
                "select " +
                "   * " +
                "from " +
                "   user_visit_action " +
                "where " +
                "   date >= '" + startTime + "'" +
                "   and date <= '" + endTime + "'";
        DataFrame actionDF = sqlContext.sql(sql);

        return actionDF.javaRDD();
    }


    //将所有包含过滤条件的数据聚合起来
    private JavaPairRDD<String, String> sessionid2ActionsRDD(JavaRDD<Row> actionRDD){
        //将actionRDD转成JavaPairRDD
        JavaPairRDD<String, Row> session2ActionRDD = actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            @Override
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(2), row);
            }
        });
        //根绝session进行聚合
        JavaPairRDD<String, Iterable<Row>> session2ActionRDDs = session2ActionRDD.groupByKey();
        //将聚合后的数据转成JavaPairRDD<UserID, acitonPartInfo>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = session2ActionRDDs.mapToPair(new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                String sessionId = tuple._1;
                Iterator<Row> iterator = tuple._2.iterator();
                StringBuffer searchKeyWordsBuffer = new StringBuffer();
                StringBuffer clickCategoryIdsBuffer = new StringBuffer();
                Long userid = null;
                while (iterator.hasNext()){
                    Row row = iterator.next();
                    if(null == userid){
                        userid = row.getLong(1);
                    }
                    String searchKeyWord = row.getString(5);
                    Long clickCategoryId = row.getLong(6);
                    if(StringUtils.isNotEmpty(searchKeyWord)){
                        if(!searchKeyWordsBuffer.toString().contains(searchKeyWord)){
                            searchKeyWordsBuffer.append(searchKeyWord).append(",");
                        }
                    }
                    if(null != clickCategoryId){
                        if(!clickCategoryIdsBuffer.toString().contains(clickCategoryId+"")){
                            clickCategoryIdsBuffer.append(clickCategoryId+"").append(",");
                        }
                    }
                }
                String searchKeyWords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                String partAggrInfo = CommonConstants.SPARK_FIELD_SESSION_ID + "=" + sessionId + "|" +
                        CommonConstants.SPARK_FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
                        CommonConstants.SPARK_FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds;
                return new Tuple2<>(userid, partAggrInfo);
            }
        });
        //获取用户数据
        String sql = "select" +
                "       *" +
                "     from" +
                "       user_info";
        JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<>(row.getLong(0), row);
            }
        });
        //进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfo = userid2PartAggrInfoRDD.join(userid2InfoRDD);
        //将join的数据转成JavaPairRDD<SessionId, actionFullInfo>
        JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfo.mapToPair(new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                Long userid = tuple._1;
                String partAggrInfo = tuple._2._1;
                Row userInfo = tuple._2._2;
                Integer age = userInfo.getInt(3);
                String professional = userInfo.getString(4);
                String city = userInfo.getString(5);
                String sex = userInfo.getString(6);
                String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", CommonConstants.SPARK_FIELD_SESSION_ID);

                partAggrInfo = partAggrInfo + "|" + CommonConstants.SPARK_FIELD_AGE + "=" + age + "|" +
                        CommonConstants.SPARK_FIELD_CITY + "+" + city + "|" +
                        CommonConstants.SPARK_FIELD_PROFESSIONAL + "=" + professional + "|" +
                        CommonConstants.SPARK_FIELD_SEX + "=" + sex;
                return new Tuple2<>(sessionid, partAggrInfo);
            }
        });
        //返回数据
        return sessionid2FullAggrInfoRDD;
    }

    /**
     * 将用户行为数据根据指定条件进行过滤
     * @param sessionid2AggrInfoRDD
     * @param taskParams
     * @return
     */
    private JavaPairRDD<String, String> filterSessionid2ActionsRDD(JavaPairRDD<String, String> sessionid2AggrInfoRDD, JSONObject taskParams){
        //获取所有的过滤参数
        Integer startAge = Integer.valueOf(ParamUtils.getParam(taskParams, CommonConstants.SPARK_PARAM_START_AGE));
        Integer endAge = Integer.valueOf(ParamUtils.getParam(taskParams, CommonConstants.SPARK_PARAM_END_AGE));
        String professionals = ParamUtils.getParam(taskParams, CommonConstants.SPARK_PARAM_PROFESSIONALS);
        String sex = ParamUtils.getParam(taskParams, CommonConstants.SPARK_PARAM_SEX);
        String cities = ParamUtils.getParam(taskParams, CommonConstants.SPARK_PARAM_CITIES);
        String searchKeyWords = ParamUtils.getParam(taskParams, CommonConstants.SPARK_PARAM_SEARCH_KEYWORDS);
        String clickCategoryIds = ParamUtils.getParam(taskParams, CommonConstants.SPARK_PARAM_CLICK_CATEGORY_IDS);

        String _parameter = (startAge != null ? CommonConstants.SPARK_PARAM_START_AGE + "=" + startAge + "|" : "")
                + (endAge != null ? CommonConstants.SPARK_PARAM_END_AGE + "=" + endAge + "|" : "")
                + (professionals != null ? CommonConstants.SPARK_PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
                + (cities != null ? CommonConstants.SPARK_PARAM_CITIES + "=" + cities + "|" : "")
                + (sex != null ? CommonConstants.SPARK_PARAM_SEX + "=" + sex + "|" : "")
                + (searchKeyWords != null ? CommonConstants.SPARK_PARAM_SEARCH_KEYWORDS+ "=" + searchKeyWords + "|" : "")
                + (clickCategoryIds != null ? CommonConstants.SPARK_PARAM_CLICK_CATEGORY_IDS + "=" + clickCategoryIds: "");
        if(_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;

        return null;
    }
}
