package com.xuehui.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.xuehui.constants.CommonConstants;
import com.xuehui.mapper.SessionAggrStatMapper;
import com.xuehui.mapper.TaskMapper;
import com.xuehui.pojo.SessionAggrStat;
import com.xuehui.pojo.Task;
import com.xuehui.untils.*;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.stereotype.Service;
import scala.Serializable;
import scala.Tuple2;

import javax.annotation.Resource;
import java.util.Date;
import java.util.Iterator;

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

    @Resource transient
    private SessionAggrStatMapper sessionAggrStatMapper;


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
        //定义一个共享累加变量
        Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
        //根绝sessioni的进行过滤
        JavaPairRDD<String, String> filterSessionid2AggrInfoRDD = filterSessionid2ActionsRDD(sessionid2AggrInfoRDD, params, sessionAggrStatAccumulator);
        filterSessionid2AggrInfoRDD.count();
        //将统计的结果持久化到mysql数据库
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());


        sc.close();
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
    private JavaPairRDD<String, String> sessionid2ActionsRDD(final JavaRDD<Row> actionRDD){
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
                Date startTime = null;
                Date endTime = null;
                Long stepLength = 0L;
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
                    //获取该session的开始时间和结束时间
                    String actionTimeStr = row.getString(4);
                    if(StringUtils.isNotEmpty(actionTimeStr)){
                        Date actionTime = DateUtils.parseDateTime(actionTimeStr);
                        if(null != actionTime){
                            if(null == startTime){
                                startTime = actionTime;
                            }
                            if(null == endTime){
                                endTime = actionTime;
                            }
                            if(actionTime.before(startTime)){
                                startTime = actionTime;
                            }
                            if(actionTime.after(endTime)){
                                endTime = actionTime;
                            }
                        }
                    }
                    //获取该session的步长
                    stepLength++;
                }
                String searchKeyWords = StringUtils.trimComma(searchKeyWordsBuffer.toString());
                String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                Long visitLength = 0L;
                if(null != startTime && null != endTime){
                    visitLength = endTime.getTime() - startTime.getTime();
                }

                String partAggrInfo = CommonConstants.SPARK_FIELD_SESSION_ID + "=" + sessionId + "|" +
                        CommonConstants.SPARK_FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
                        CommonConstants.SPARK_FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" +
                        CommonConstants.SPARK_FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
                        CommonConstants.SPARK_FIELD_STEP_LENGTH + "=" + stepLength;
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
    private JavaPairRDD<String, String> filterSessionid2ActionsRDD(JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                                                   JSONObject taskParams,
                                                                   final Accumulator<String> sessionAggrStatAccumulator){
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
        JavaPairRDD<String, String> filterSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String aggregateInfo = tuple._2;
                if(!ValidUtils.between(aggregateInfo, CommonConstants.SPARK_FIELD_AGE, parameter, CommonConstants.SPARK_PARAM_START_AGE, CommonConstants.SPARK_PARAM_END_AGE)){
                    return false;
                }

                if (!ValidUtils.in(aggregateInfo, CommonConstants.SPARK_FIELD_CITY, parameter, CommonConstants.SPARK_PARAM_CITIES)) {
                    return false;
                }

                if(!ValidUtils.in(aggregateInfo, CommonConstants.SPARK_FIELD_PROFESSIONAL, parameter, CommonConstants.SPARK_PARAM_PROFESSIONALS)){
                    return false;
                }

                if (!ValidUtils.equal(aggregateInfo, CommonConstants.SPARK_FIELD_SEX, parameter, CommonConstants.SPARK_PARAM_SEX)) {
                    return false;
                }

                if (!ValidUtils.in(aggregateInfo, CommonConstants.SPARK_FIELD_SEARCH_KEYWORDS, parameter, CommonConstants.SPARK_PARAM_SEARCH_KEYWORDS)) {
                    return false;
                }

                if(!ValidUtils.in(aggregateInfo, CommonConstants.SPARK_FIELD_CLICK_CATEGORY_IDS, parameter, CommonConstants.SPARK_FIELD_CLICK_CATEGORY_IDS)){
                    return false;
                }

                sessionAggrStatAccumulator.add(CommonConstants.SESSION_COUNT);
                //对符合要求的用户行为数据进行聚合统计
                Long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggregateInfo, "\\|", CommonConstants.SPARK_FIELD_STEP_LENGTH));
                Long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggregateInfo, "\\|", CommonConstants.SPARK_FIELD_VISIT_LENGTH));
                calculateVisitLength(visitLength);
                calculateStepLength(stepLength);
                return true;
            }
            private void calculateVisitLength(long visitLength) {
                if(visitLength >=1 && visitLength <= 3) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_1s_3s);
                } else if(visitLength >=4 && visitLength <= 6) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_4s_6s);
                } else if(visitLength >=7 && visitLength <= 9) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_7s_9s);
                } else if(visitLength >=10 && visitLength <= 30) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_10s_30s);
                } else if(visitLength > 30 && visitLength <= 60) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_30s_60s);
                } else if(visitLength > 60 && visitLength <= 180) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_1m_3m);
                } else if(visitLength > 180 && visitLength <= 600) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_3m_10m);
                } else if(visitLength > 600 && visitLength <= 1800) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_10m_30m);
                } else if(visitLength > 1800) {
                    sessionAggrStatAccumulator.add(CommonConstants.TIME_PERIOD_30m);
                }
            }
            private void calculateStepLength(long stepLength) {
                if(stepLength >= 1 && stepLength <= 3) {
                    sessionAggrStatAccumulator.add(CommonConstants.STEP_PERIOD_1_3);
                } else if(stepLength >= 4 && stepLength <= 6) {
                    sessionAggrStatAccumulator.add(CommonConstants.STEP_PERIOD_4_6);
                } else if(stepLength >= 7 && stepLength <= 9) {
                    sessionAggrStatAccumulator.add(CommonConstants.STEP_PERIOD_7_9);
                } else if(stepLength >= 10 && stepLength <= 30) {
                    sessionAggrStatAccumulator.add(CommonConstants.STEP_PERIOD_10_30);
                } else if(stepLength > 30 && stepLength <= 60) {
                    sessionAggrStatAccumulator.add(CommonConstants.STEP_PERIOD_30_60);
                } else if(stepLength > 60) {
                    sessionAggrStatAccumulator.add(CommonConstants.STEP_PERIOD_60);
                }
            }
        });
        return filterSessionid2AggrInfoRDD;
    }


    private void calculateAndPersistAggrStat(String value, long taskid) {
        // 从Accumulator统计串中获取值
        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_3m_10m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(
                value, "\\|", CommonConstants.STEP_PERIOD_60));

        // 计算各个访问时长和访问步长的范围
        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double)visit_length_1s_3s / (double)session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double)visit_length_4s_6s / (double)session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double)visit_length_7s_9s / (double)session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double)visit_length_10s_30s / (double)session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double)visit_length_30s_60s / (double)session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double)visit_length_1m_3m / (double)session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double)visit_length_3m_10m / (double)session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_10m_30m / (double)session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double)visit_length_30m / (double)session_count, 2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double)step_length_1_3 / (double)session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double)step_length_4_6 / (double)session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double)step_length_7_9 / (double)session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double)step_length_10_30 / (double)session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double)step_length_30_60 / (double)session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                step_length_60 / (double)session_count, 2);

        // 将统计结果封装为Domain对象
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        sessionAggrStatMapper.insert(sessionAggrStat);
    }
}
