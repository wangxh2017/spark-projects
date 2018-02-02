/************************************************************************************
 * Created at 2018/02/01                                                                       
 * spark学习
 *
 * 项目名称：用户行为数据分析                                                        
 * 版权说明：本代码仅供学习使用                           
 ************************************************************************************/
package com.xuehui;

import com.xuehui.spark.session.UserVisitSessionAnalyzeSpark;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author 王雪辉
 * @version 2.0
 * @Package com.xuehui
 * @Description: 主程序测试用例
 * @date 16:09 : 2018/2/1
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring-mybatis.xml" })
public class UserVisitSessionAnalyzeSparkTest {

    @Autowired
    private UserVisitSessionAnalyzeSpark userVisitSessionAnalyzeSpark;


    @Test
    public void test(){
        userVisitSessionAnalyzeSpark.run(new String[]{"1"});
    }

}
