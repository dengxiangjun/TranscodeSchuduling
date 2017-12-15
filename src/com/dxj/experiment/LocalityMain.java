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

        for (int i = 0; i < 80; i++) {

            int complexity = Random.nextInt(40, 200);
            double segmentSize = complexity * Random.nextDouble(0.8, 1.2);
            int rackNum = Random.nextInt(0, rackCount), otherRackNum = Random.nextInt(0, rackCount);
            while (rackNum == otherRackNum) otherRackNum = (rackNum + Random.nextInt(0, rackCount)) % rackCount;

            Rack rack = racks.get(rackNum), otherRack = racks.get(otherRackNum);
            List<Node> rackNodes = rack.getNodes(), otherRackNodes = otherRack.getNodes();
            List<Node> location = TaskUtil.segmentPlacement(rackNodes, otherRackNodes);

            Task task = new Task("task_" + i, complexity, segmentSize, location);
            task.initSubTask(5, 20);
            tasks.add(task);

            for (Node node : location) {
                List<Task> storageTasks = node.getStorageTasks();
                storageTasks.add(task);
                node.setStorageTasks(storageTasks);
            }
        }

        Job job = new Job(nodes, tasks,racks);

        LMCT lmct = new LMCT();
        double lmctFt = lmct.schedule(job);
        System.out.println("LMCT算法调度结果: " + lmctFt + " ;makespan "+ job.getMakespan());

        JobUtil.clear(job);

        LMaxMCT lMaxMCT = new LMaxMCT();
        double localityMaxMCTFt = lMaxMCT.schedule(job);
        System.out.println("LocalityMaxMCT算法调度结果: " + localityMaxMCTFt + " ;makespan "+ job.getMakespan());

        JobUtil.clear(job);

//        NewMaxMCT newMaxMCT = new NewMaxMCT();
//        double newMaxMCTFt = newMaxMCT.schedule(job);
//        System.out.println("NewMaxMCT算法调度结果: " + newMaxMCTFt);
//
//        JobUtil.clear(job);

        LMLFT lmlft = new LMLFT();
        double lmlftFt = lmlft.schedule(job);
        System.out.println("LMLFT算法调度结果: " + lmlftFt + " ;makespan "+ job.getMakespan());
        JobUtil.clear(job);

//        LMLFT3 lmlft3 = new LMLFT3();
//        double lmlft3Ft = lmlft3.schedule(job);
//        System.out.println("LMLFT3算法调度结果: " + lmlft3Ft);
//        JobUtil.clear(job);
//
//        LMLFT1 lmlft1 = new LMLFT1();
//        double lmlft1Ft = lmlft1.schedule(job);
//        System.out.println("LMLFT1算法调度结果: " + lmlft1Ft);
//        JobUtil.clear(job);

//        BSTMCT bstmct = new BSTMCT();
//        double bstmctFt = bstmct.schedule(job);
//        System.out.println("BSTMCT算法调度结果: " + bstmctFt);
//        JobUtil.clear(job);

        PLTS plts = new PLTS();
        double pltsFt = plts.schedule(job);
        System.out.println("PLTS算法调度结果: " + pltsFt + " ;makespan "+ job.getMakespan());

        JobUtil.clear(job);

//        LMergeMaxMCT lMergeMaxMCT = new LMergeMaxMCT();
//        double lMergeMaxMCTFt = lMergeMaxMCT.schedule(job);
//        System.out.println("LMergeMaxMCT算法调度结果: " + lMergeMaxMCTFt);
//        RankMCT rankMCT = new RankMCT();
//        double rankMCTFt = rankMCT.schedule(job);
//        System.out.println("RankMCT算法调度结果: " + rankMCTFt);
//
//        JobUtil.clear(job);
//        HJ hj = new HJ();
//        double hjFt = hj.schedule(job);
//        System.out.println("HJ算法调度结果: " + hjFt);
//
//        JobUtil.clear(job);
//        HJ1 hj1 = new HJ1();
//        double hj1Ft = hj1.schedule(job);
//        System.out.println("HJ1算法调度结果: " + hj1Ft);
//
//        JobUtil.clear(job);
//        HJ2 hj2 = new HJ2();
//        double hj2Ft = hj2.schedule(job);
//        System.out.println("HJ2算法调度结果: " + hj2Ft);
//
//        JobUtil.clear(job);
//        HJ3 hj3 = new HJ3();
//        double hj3Ft = hj3.schedule(job);
//        System.out.println("HJ3算法调度结果: " + hj3Ft);
//
//        JobUtil.clear(job);
//        HJ4 hj4 = new HJ4();
//        double hj4Ft = hj4.schedule(job);
//        System.out.println("HJ4算法调度结果: " + hj4Ft);

        JobUtil.clear(job);
        BS_EFT bs_eft = new BS_EFT();
        double bs_eftFt = bs_eft.schedule(job);
        System.out.println("BS_EFT算法调度结果: " + bs_eftFt + " ;makespan "+ job.getMakespan());
    }
}
