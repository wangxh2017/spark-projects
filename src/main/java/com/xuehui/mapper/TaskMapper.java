package com.xuehui.mapper;

import com.xuehui.pojo.Task;

/**
 * Created by Administrator on 2018/2/1.
 */
public interface TaskMapper {
    /**
     * 根据任务id获取任务信息
     * @param taskId
     * @return
     */
    Task findTaskByTaskId(Long taskId);
}