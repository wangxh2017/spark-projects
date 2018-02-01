package com.xuehui;

import com.xuehui.mapper.UserMapper;
import com.xuehui.pojo.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by Administrator on 2018/2/1.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring-mybatis.xml" })
public class UserTest {

    @Autowired
    private UserMapper userMapper;

    @Test
    public void testSelect(){
        User user = userMapper.select("zhangsan");
        System.out.println(user);
    }
}
