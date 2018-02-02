/************************************************************************************
 * Created at 2018/02/02                                                                       
 * spark学习                                                                     
 *
 * 项目名称：用户行为数据分析                                                        
 * 版权说明：本代码仅供学习使用                           
 ************************************************************************************/
package com.xuehui.pojo;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.Serializable;

/**
 * @author 王雪辉
 * @version 2.0
 * @Package com.xuehui.pojo
 * @Description: 用户行为数据统计指标
 * @date 14:42 : 2018/2/2
 */
public class SessionAggrStat implements Serializable{
        private Long taskid;
        private Long session_count;
        private Double visit_length_1s_3s_ratio;
        private Double visit_length_4s_6s_ratio;
        private Double visit_length_7s_9s_ratio;
        private Double visit_length_10s_30s_ratio;
        private Double visit_length_30s_60s_ratio;
        private Double visit_length_1m_3m_ratio;
        private Double visit_length_3m_10m_ratio;
        private Double visit_length_10m_30m_ratio;
        private Double visit_length_30m_ratio;
        private Double step_length_1_3_ratio;
        private Double step_length_4_6_ratio;
        private Double step_length_7_9_ratio;
        private Double step_length_10_30_ratio;
        private Double step_length_30_60_ratio;
        private Double step_length_60_ratio;

    public Long getTaskid() {
        return taskid;
    }

    public void setTaskid(Long taskid) {
        this.taskid = taskid;
    }

    public Long getSession_count() {
        return session_count;
    }

    public void setSession_count(Long session_count) {
        this.session_count = session_count;
    }

    public Double getVisit_length_1s_3s_ratio() {
        return visit_length_1s_3s_ratio;
    }

    public void setVisit_length_1s_3s_ratio(Double visit_length_1s_3s_ratio) {
        this.visit_length_1s_3s_ratio = visit_length_1s_3s_ratio;
    }

    public Double getVisit_length_4s_6s_ratio() {
        return visit_length_4s_6s_ratio;
    }

    public void setVisit_length_4s_6s_ratio(Double visit_length_4s_6s_ratio) {
        this.visit_length_4s_6s_ratio = visit_length_4s_6s_ratio;
    }

    public Double getVisit_length_7s_9s_ratio() {
        return visit_length_7s_9s_ratio;
    }

    public void setVisit_length_7s_9s_ratio(Double visit_length_7s_9s_ratio) {
        this.visit_length_7s_9s_ratio = visit_length_7s_9s_ratio;
    }

    public Double getVisit_length_10s_30s_ratio() {
        return visit_length_10s_30s_ratio;
    }

    public void setVisit_length_10s_30s_ratio(Double visit_length_10s_30s_ratio) {
        this.visit_length_10s_30s_ratio = visit_length_10s_30s_ratio;
    }

    public Double getVisit_length_30s_60s_ratio() {
        return visit_length_30s_60s_ratio;
    }

    public void setVisit_length_30s_60s_ratio(Double visit_length_30s_60s_ratio) {
        this.visit_length_30s_60s_ratio = visit_length_30s_60s_ratio;
    }

    public Double getVisit_length_1m_3m_ratio() {
        return visit_length_1m_3m_ratio;
    }

    public void setVisit_length_1m_3m_ratio(Double visit_length_1m_3m_ratio) {
        this.visit_length_1m_3m_ratio = visit_length_1m_3m_ratio;
    }

    public Double getVisit_length_3m_10m_ratio() {
        return visit_length_3m_10m_ratio;
    }

    public void setVisit_length_3m_10m_ratio(Double visit_length_3m_10m_ratio) {
        this.visit_length_3m_10m_ratio = visit_length_3m_10m_ratio;
    }

    public Double getVisit_length_10m_30m_ratio() {
        return visit_length_10m_30m_ratio;
    }

    public void setVisit_length_10m_30m_ratio(Double visit_length_10m_30m_ratio) {
        this.visit_length_10m_30m_ratio = visit_length_10m_30m_ratio;
    }

    public Double getVisit_length_30m_ratio() {
        return visit_length_30m_ratio;
    }

    public void setVisit_length_30m_ratio(Double visit_length_30m_ratio) {
        this.visit_length_30m_ratio = visit_length_30m_ratio;
    }

    public Double getStep_length_1_3_ratio() {
        return step_length_1_3_ratio;
    }

    public void setStep_length_1_3_ratio(Double step_length_1_3_ratio) {
        this.step_length_1_3_ratio = step_length_1_3_ratio;
    }

    public Double getStep_length_4_6_ratio() {
        return step_length_4_6_ratio;
    }

    public void setStep_length_4_6_ratio(Double step_length_4_6_ratio) {
        this.step_length_4_6_ratio = step_length_4_6_ratio;
    }

    public Double getStep_length_7_9_ratio() {
        return step_length_7_9_ratio;
    }

    public void setStep_length_7_9_ratio(Double step_length_7_9_ratio) {
        this.step_length_7_9_ratio = step_length_7_9_ratio;
    }

    public Double getStep_length_10_30_ratio() {
        return step_length_10_30_ratio;
    }

    public void setStep_length_10_30_ratio(Double step_length_10_30_ratio) {
        this.step_length_10_30_ratio = step_length_10_30_ratio;
    }

    public Double getStep_length_30_60_ratio() {
        return step_length_30_60_ratio;
    }

    public void setStep_length_30_60_ratio(Double step_length_30_60_ratio) {
        this.step_length_30_60_ratio = step_length_30_60_ratio;
    }

    public Double getStep_length_60_ratio() {
        return step_length_60_ratio;
    }

    public void setStep_length_60_ratio(Double step_length_60_ratio) {
        this.step_length_60_ratio = step_length_60_ratio;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }
}
