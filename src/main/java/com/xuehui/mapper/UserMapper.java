package com.xuehui.mapper;

import com.xuehui.pojo.User;

/**
 * Created by Administrator on 2018/2/1.
 */
public interface UserMapper {
    User select(String userName);
}
