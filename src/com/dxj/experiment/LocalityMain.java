package com.dxj.experiment;

import com.dxj.model.Job;
import com.dxj.model.Node;
import com.dxj.model.Rack;
import com.dxj.model.Task;
import com.dxj.scheduler.locality.*;
import com.dxj.util.RackUtil;
import com.dxj.util.Random;
import com.dxj.util.JobUtil;
import com.dxj.util.TaskUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * 考虑分片的通信开销和数据本地性
 */
public class LocalityMain {

    public static void main(String[] args) {
        List<Node> nodes = new ArrayList<>();
        List<Task> tasks = new ArrayList<>();

        int rackCount = 3;
        List<Rack> racks = new ArrayList<>();
        for (int i = 0; i < rackCount; i++) {
            racks.add(new Rack(i));
        }

        for (int i = 0; i < 10; i++) {
            double capacity = Random.nextDouble(1.0, 5.0);
            Rack rack = racks.get(Random.nextInt(0, rackCount));//随机选取一个机架

            Node node = new Node("node_" + i, capacity, rack);
            nodes.add(node);
            List<Node> rackNodes = rack.getNodes();
            rackNodes.add(node);
            rack.setNodes(rackNodes);
        }

        RackUtil.checkNodesDistribution(racks);

        for (int i = 0; i < 250; i++) {

            int complexity = Random.nextInt(60, 600);
            double segmentSize = complexity * Random.nextDouble(0.8, 1.2);
            int rackNum = Random.nextInt(0, rackCount), otherRackNum = Random.nextInt(0, rackCount);
            while (rackNum == otherRackNum) otherRackNum = (rackNum + Random.nextInt(0, rackCount)) % rackCount;

            Rack rack = racks.get(rackNum), otherRack = racks.get(otherRackNum);
            List<Node> rackNodes = rack.getNodes(), otherRackNodes = otherRack.getNodes();
            List<Node> location = TaskUtil.segmentPlacement(rackNodes, otherRackNodes);

            Task task = new Task("task_" + i, complexity, segmentSize, location);
            task.initSubTask(1, 20);
            tasks.add(task);

            for (Node node : location) {
                List<Task> storageTasks = node.getStorageTasks();
                storageTasks.add(task);
                node.setStorageTasks(storageTasks);
            }
        }

        Job job = new Job(nodes, tasks);

        LMCT lmct = new LMCT();
        double lmctFt = lmct.schedule(job);
        System.out.println("LMCT算法调度结果: " + lmctFt);

        JobUtil.clear(job);

        LMaxMCT lMaxMCT = new LMaxMCT();
        double localityMaxMCTFt = lMaxMCT.schedule(job);
        System.out.println("LocalityMaxMCT算法调度结果: " + localityMaxMCTFt);

        JobUtil.clear(job);

        LMLFT lmlft = new LMLFT();
        double lmlftFt = lmlft.schedule(job);
        System.out.println("LMLFT算法调度结果: " + lmlftFt);
        JobUtil.clear(job);

        LMLFT1 lmlft1 = new LMLFT1();
        double lmlft1Ft = lmlft1.schedule(job);
        System.out.println("LMLFT1算法调度结果: " + lmlft1Ft);
        JobUtil.clear(job);

        BSTMCT bstmct = new BSTMCT();
        double bstmctFt = bstmct.schedule(job);
        System.out.println("LMLFT2算法调度结果: " + bstmctFt);
        JobUtil.clear(job);

        PLTS plts = new PLTS();
        double pltsFt = plts.schedule(job);
        System.out.println("PLTS算法调度结果: " + pltsFt);

        JobUtil.clear(job);

        LMergeMaxMCT lMergeMaxMCT = new LMergeMaxMCT();
        double lMergeMaxMCTFt = lMergeMaxMCT.schedule(job);
        System.out.println("LMergeMaxMCT算法调度结果: " + lMergeMaxMCTFt);
    }
}
