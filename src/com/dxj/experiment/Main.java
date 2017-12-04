package com.dxj.experiment;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Task;
import com.dxj.scheduler.*;
import com.dxj.util.JobUtil;
import com.dxj.util.Random;

import java.util.ArrayList;
import java.util.List;

/**
 * 不考虑分片的通信开销和数据本地性
 */
public class Main {

    public static void main(String[] args) {
        List<Node> nodes = new ArrayList<>();
        List<Task> tasks = new ArrayList<>();

        for (int i = 0; i < 10; i++) {//
            double capacity = Random.nextDouble(1.0, 3.0);
            Node node = new Node("node_" + i, capacity);
            nodes.add(node);
        }

        for (int i = 0; i < 250; i++) {
            int complexity = Random.nextInt(15, 300);
            Task task = new Task("task_" + i, complexity);
            task.initSubTask(1, 150);
            tasks.add(task);
        }

        Job job = new Job(nodes, tasks);

        MCT mct = new MCT();
        double mctFt = mct.schedule(job);
        System.out.println("MCT算法调度结果" + mctFt);
        JobUtil.clear(job);

        MaxMCT maxMCT = new MaxMCT();
        double maxMCTFt = maxMCT.schedule(job);
        System.out.println("MaxMCT算法调度结果" + maxMCTFt);
        JobUtil.clear(job);

        MLFT mlft = new MLFT();
        double mlftFt = mlft.schedule(job);
        System.out.println("MLFT算法调度结果" + mlftFt);
        JobUtil.clear(job);

        CoarseGrainedSegment cgs = new CoarseGrainedSegment();
        double cgsFt = cgs.schedule(job);
        System.out.println("粗粒度分片算法调度结果" + cgsFt);
        JobUtil.clear(job);

        MergeMaxMCT mergeMaxMCT = new MergeMaxMCT();
        double mmmFt = mergeMaxMCT.schedule(job);
        System.out.println("归并MaxMCT算法调度结果" + mmmFt);
    }
}
