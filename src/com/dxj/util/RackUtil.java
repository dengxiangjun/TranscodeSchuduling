package com.dxj.util;

import com.dxj.model.Node;
import com.dxj.model.Rack;

import java.util.ArrayList;
import java.util.List;

/**
 * 机架工具
 * Created by deng on 2017/11/28.
 */
public class RackUtil {
    /**
     * 检验机架上结点的分布状况，将机架上结点最少的和最多的平均一下
     * @param racks 系统机架集
     */
    public static void checkNodesDistribution(List<Rack> racks){
        int maxCnt = Integer.MIN_VALUE, minCnt = Integer.MAX_VALUE;
        Rack maxCntRack = null, minCntRack = null;
        for (Rack rack : racks){
            int nodeCnt = rack.getNodes().size();
            if (nodeCnt > maxCnt){
                maxCnt = nodeCnt;
                maxCntRack = rack;
            }
            if (nodeCnt < minCnt){
                minCnt = nodeCnt;
                minCntRack = rack;
            }
        }

        List<Node> nodes = new ArrayList<>();
        nodes.addAll(maxCntRack.getNodes());
        nodes.addAll(minCntRack.getNodes());
        maxCntRack.setNodes(new ArrayList<>());
        minCntRack.setNodes(new ArrayList<>());
        for (int i =0; i< nodes.size();i++){
            if (i % 2 == 0) {
                List<Node> rackNodes = maxCntRack.getNodes();
                rackNodes.add(nodes.get(i));
                maxCntRack.setNodes(rackNodes);
            }else {
                List<Node> rackNodes = minCntRack.getNodes();
                rackNodes.add(nodes.get(i));
                minCntRack.setNodes(rackNodes);
            }
        }
    }
}
