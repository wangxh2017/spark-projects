/************************************************************************************
 * Created at 2018/02/02                                                                       
 * spark学习
 *
 * 项目名称：用户行为数据分析                                                        
 * 版权说明：本代码仅供学习使用                           
 ************************************************************************************/
package com.xuehui.spark.session;

import com.xuehui.constants.CommonConstants;
import com.xuehui.untils.StringUtils;
import org.apache.spark.AccumulatorParam;

/**
 * @author 王雪辉
 * @version 2.0
 * @Package com.xuehui.spark.session
 * @Description:
 * @date 13:45 : 2018/2/2
 */
public class SessionAggrStatAccumulator implements AccumulatorParam<String> {

    @Override
    public String zero(String initialValue) {
        return CommonConstants.SESSION_COUNT + "=0|"
                + CommonConstants.TIME_PERIOD_1s_3s + "=0|"
                + CommonConstants.TIME_PERIOD_4s_6s + "=0|"
                + CommonConstants.TIME_PERIOD_7s_9s + "=0|"
                + CommonConstants.TIME_PERIOD_10s_30s + "=0|"
                + CommonConstants.TIME_PERIOD_30s_60s + "=0|"
                + CommonConstants.TIME_PERIOD_1m_3m + "=0|"
                + CommonConstants.TIME_PERIOD_3m_10m + "=0|"
                + CommonConstants.TIME_PERIOD_10m_30m + "=0|"
                + CommonConstants.TIME_PERIOD_30m + "=0|"
                + CommonConstants.STEP_PERIOD_1_3 + "=0|"
                + CommonConstants.STEP_PERIOD_4_6 + "=0|"
                + CommonConstants.STEP_PERIOD_7_9 + "=0|"
                + CommonConstants.STEP_PERIOD_10_30 + "=0|"
                + CommonConstants.STEP_PERIOD_30_60 + "=0|"
                + CommonConstants.STEP_PERIOD_60 + "=0";
    }

    @Override
    public String addAccumulator(String t1, String t2) {
        return add(t1, t2);
    }

    @Override
    public String addInPlace(String r1, String r2) {
        return add(r1, r2);
    }

    private String add(String s1, String s2){
        if(StringUtils.isEmpty(s1)){
            return s2;
        }
        String oldValue = StringUtils.getFieldFromConcatString(s1, "\\|", s2);
        if(StringUtils.isNotEmpty(oldValue)){
            int newValue = Integer.valueOf(oldValue) + 1;
            return StringUtils.setFieldInConcatString(s1, "\\|", s2, newValue+"");
        }
        return s1;
    }

}
