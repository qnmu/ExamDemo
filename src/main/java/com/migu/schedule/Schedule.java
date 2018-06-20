package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;
import com.sun.jmx.remote.internal.ArrayQueue;
import com.sun.org.apache.bcel.internal.generic.RETURN;

import java.util.*;

/*
*类名和方法不能修改
 */
public class Schedule {

    List<Integer> nodeIds = new ArrayList<Integer>();
    List<Integer> taskIds = new ArrayList<Integer>();
    List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
    ArrayQueue<Integer> taskQueue = new ArrayQueue<Integer>(50);
    Map<Integer,Integer> taskConsumptionMap = new HashMap<Integer,Integer>();
    Map<Integer,Integer> nodeConsumptionMap = new HashMap<Integer,Integer>();

    public int init() {
        // TODO 方法未实现
        nodeIds.clear();
        taskIds.clear();
        taskInfos.clear();
        taskQueue.clear();
        taskConsumptionMap.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        // TODO 方法未实现
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }
        if (nodeIds.contains(Integer.valueOf(nodeId))) {
            return ReturnCodeKeys.E005;
        }
        nodeIds.add(Integer.valueOf(nodeId));

        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        // TODO 方法未实现
        if (nodeId <= 0) {
            return ReturnCodeKeys.E004;
        }
        if (!nodeIds.contains(Integer.valueOf(nodeId))) {
            return ReturnCodeKeys.E007;
        } else {
            if (hasTaskOnNode(nodeId)) {
                hangUpTask(nodeId);
            }
            nodeIds.remove(Integer.valueOf(nodeId));
        }
        return ReturnCodeKeys.E006;
    }

    //挂起任务
    private void hangUpTask(int nodeId) {
        List<Integer> tasks = getTasks(nodeId);
        for (Integer taskId : tasks) {
            //添加到挂起队列
            taskQueue.add(Integer.valueOf(taskId));
            //清理task与node关联关系
            hangUpTaskInfo(taskId);
        }
    }
    //挂起任务
    private void hangUpTaskInfo(int taskId){
        for(TaskInfo taskInfo : taskInfos){
            if(taskInfo.getTaskId() == taskId){
                taskInfo.setNodeId(-1);
            }
        }
    }
    //清理task与node关联关系
    private void clearTaskAndNodeRelation(int taskId){
        for(TaskInfo taskInfo : taskInfos){
            if(taskInfo.getTaskId() == taskId){
                taskInfos.remove(taskInfo);
            }
        }
    }

    //node是否有task
    private boolean hasTaskOnNode(int nodeId) {
        for (TaskInfo taskInfo : taskInfos) {
            if (taskInfo.getNodeId() == nodeId) {
                return true;
            }
        }
        return false;
    }

    //获得node上的任务
    private List<Integer> getTasks(int nodeId) {
        List<Integer> tasks = new ArrayList<Integer>();
        for (TaskInfo taskInfo : taskInfos) {
            if (taskInfo.getNodeId() == nodeId) {
                tasks.add(taskInfo.getTaskId());
            }
        }
        return tasks;
    }

    public int addTask(int taskId, int consumption) {
        // TODO 方法未实现
        if(taskId <= 0){
            return ReturnCodeKeys.E009;
        }
        if(taskIds.contains(Integer.valueOf(taskId))){
            return ReturnCodeKeys.E010;
        }
        taskIds.add(Integer.valueOf(taskId));
        taskConsumptionMap.put(Integer.valueOf(taskId),Integer.valueOf(consumption));
        taskQueue.add(Integer.valueOf(taskId));
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        // TODO 方法未实现
        if(taskId <=0){
            return ReturnCodeKeys.E009;
        }
        if(!taskIds.contains(Integer.valueOf(taskId))){
            return ReturnCodeKeys.E012;
        }
        clearTaskAndNodeRelation(taskId);
        taskIds.remove(taskIds);
        taskQueue.remove(Integer.valueOf(taskId));
        taskConsumptionMap.remove(Integer.valueOf(taskId));
        return ReturnCodeKeys.E011;
    }


    //threshold 阈值
    public int scheduleTask(int threshold) {
        // TODO 方法未实现
        if(threshold <=0){
            return ReturnCodeKeys.E002;
        }

        //TODO 调度算法
        //如果挂起队列有任务
        if(taskQueue.size()> 0 ){

        }else{

        }

        cal();
        return ReturnCodeKeys.E013;
    }


    private int cal(){
        int taskNum = taskIds.size();
        int nodeNum = nodeIds.size();
        //任务分组数
        int fenzuNum = taskNum/nodeNum;
        //所有任务消耗量和
        int totalConsumption = 0;
        for(int i=0;i<taskIds.size();i++){
            int consu = taskConsumptionMap.get(taskIds.get(i));
            totalConsumption += consu;
        }
        //分组后消耗量和的中间值
        int pingjun = totalConsumption/fenzuNum;

        //
        List<List<Integer>> fzList = new ArrayList<List<Integer>>();
        List<Map.Entry<Integer, Integer>> taskConsumptionList = sortMap();

        //nodeid,taskid,消耗量
        LinkedHashMap<Integer,Map<Integer, Integer>> maps = new LinkedHashMap<Integer, Map<Integer, Integer>>();
        int num = 0;
        for(int i=1;i<=nodeIds.size();i++){
            Map<Integer,Integer> mapp = new HashMap<Integer, Integer>();
            for(int j=0;i<fenzuNum;i++){
                Map.Entry<Integer, Integer> me = taskConsumptionList.get(i+j);
               mapp.put(me.getKey(),me.getValue());
               num++;
            }
            maps.put(nodeIds.get(i-1),mapp);
        }
        if(num < taskConsumptionList.size()){
            Map<Integer,Integer> mapp = new HashMap<Integer, Integer>();
            Map.Entry<Integer, Integer> me = taskConsumptionList.get(num);
            mapp.put(me.getKey(),me.getValue());
            maps.put(nodeIds.get(0),mapp);
        }

        List<TaskInfo> taskInfosTemp = new ArrayList<TaskInfo>();
        for(TaskInfo taskInfo :taskInfos){
            TaskInfo taskInfoTemp = new TaskInfo();
            taskInfoTemp.setTaskId(taskInfo.getTaskId());
            taskInfoTemp.setNodeId(taskInfo.getNodeId());
            taskInfosTemp.add(taskInfoTemp);
        }
        for(Integer key :maps.keySet()){
            //taskid
            Map<Integer, Integer> map = maps.get(key);
            for(TaskInfo taskInfo :taskInfosTemp){
                if(taskInfo.getNodeId() == key){
                    taskInfos.remove(taskInfo);
                }
            }
            Set<Integer> taskKeySet = map.keySet();
            for(Integer taskKey : taskKeySet){
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.setTaskId(taskKey);
                taskInfo.setNodeId(key);
                taskInfos.add(taskInfo);
            }
        }

        return 0;
    }

    //map按值排序
    private List<Map.Entry<Integer, Integer>> sortMap(){
        List<Map.Entry<Integer, Integer>> taskConsumptionList = new ArrayList<Map.Entry<Integer, Integer>>(taskConsumptionMap.entrySet());
        Collections.sort(taskConsumptionList, new Comparator<Map.Entry<Integer, Integer>>() {
            public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        return taskConsumptionList;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        // TODO 方法未实现
        tasks.clear();
        for(TaskInfo taskInfo : taskInfos){
            tasks.add(taskInfo);
        }
        if(tasks.size() == 0){
            return ReturnCodeKeys.E016;
        }
        sortTasks(tasks);
        return ReturnCodeKeys.E015;
    }
    //按任务编号升序排列
    private void sortTasks(List<TaskInfo> tasks){
        if(tasks==null || tasks.size()==0) return ;
        int[] taskIdArray = new int[tasks.size()];
        LinkedHashMap<Integer,TaskInfo> linkedHashMap = new LinkedHashMap<Integer,TaskInfo>();
        int index = 0;
        for(int i=0;i<tasks.size();i++){
            TaskInfo taskInfo =  tasks.get(i);
            int taskId = taskInfo.getTaskId();
            taskIdArray[index++] = taskId;
            linkedHashMap.put(taskId,taskInfo);
        }
        bubbleSort(taskIdArray);
        for(int i=0;i<taskIdArray.length;i++){
            tasks.add(i,linkedHashMap.get(i));
        }
    }

    //排序
    private static void bubbleSort(int[] numbers)
    {
        int temp = 0;
        int size = numbers.length;
        for(int i = 0 ; i < size-1; i ++)
        {
            for(int j = 0 ;j < size-1-i ; j++)
            {
                if(numbers[j] > numbers[j+1])  //交换两数位置
                {
                    temp = numbers[j];
                    numbers[j] = numbers[j+1];
                    numbers[j+1] = temp;
                }
            }
        }
    }

}
