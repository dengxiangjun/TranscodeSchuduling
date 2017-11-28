package com.dxj.util;

import com.dxj.model.Job;
import com.dxj.model.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * 作业工具类
 * Created by deng on 2017/11/28.
 */
public class JobUtil {

    /**
     * 清除调度结果
     * @param job 作业
     */
    public static void clear(Job job) {
        List<Node> nodes = job.getNodes();
        for (Node node : nodes) {
            node.setTasks(new ArrayList<>());
            node.setFt(0d);
        }
    }
}
