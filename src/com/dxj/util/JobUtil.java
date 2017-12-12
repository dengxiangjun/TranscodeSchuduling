package com.dxj.util;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
        List<Task> tasks = job.getTasks();
        Collections.sort(tasks, new Comparator<Task>() {
            @Override
            public int compare(Task o1, Task o2) {
                int index1 = Integer.valueOf(o1.getName().substring(5)),index2 = Integer.valueOf(o2.getName().substring(5));
                return index1 >= index2 ? 1:-1;
            }
        });

        for (Task task : tasks){
            task.setExecuteNode(null);
            task.setMakespan(0);
            task.setComm(0);
        }
    }
}
