package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.*;

/*
 * 类名和方法不能修改
 */
public class Schedule {

  /**
   * 服务集合
   */
  private List<Integer> nodeList = new ArrayList<Integer>();

  /**
   * 任务集合
   */

  private List<Integer> tasksList = new ArrayList<Integer>();

  /**
   * 任务状态集合
   */
  private Map<Integer, List<TaskInfo>> taskStatusMap = new HashMap<Integer, List<TaskInfo>>();

  /**
   * 任务集合
   */
  private Map<Integer, Integer> tasksMap = new LinkedHashMap<Integer, Integer>();


  /**
   * 阈值
   */
  private int threshold = -1;

  /**
   * 任务状态排序
   */
  Comparator<TaskInfo> comparatorTask = new Comparator<TaskInfo>() {
    public int compare(TaskInfo taskInfo1, TaskInfo taskInfo2) {
      return (taskInfo1.getTaskId() - taskInfo2.getTaskId());
    }
  };

  /**
   * 任务排序
   */
  Comparator<Integer> comparatorTasksMap = new Comparator<Integer>() {
    public int compare(Integer taskInfo1, Integer taskInfo2) {
      return (tasksMap.get(taskInfo2) - tasksMap.get(taskInfo1));
    }
  };

  /**
   * 
   * 初始化
   * 
   * 功能说明: 系统初始化，会清空所有数据，包括已经注册到系统的服务节点信息、以及添加的任务信息，全部都被清理。执行该命令后，系统恢复到最初始的状态。
   * 
   * 参数说明： 无
   * 
   * 输出说明： 初始化成功，返回E001初始化成功。 未做此题返回 E000方法未实现。
   *
   * @author dubx
   * @return
   */
  public int init() {
    nodeList.clear();
    tasksList.clear();
    taskStatusMap.clear();
    tasksMap.clear();
    return ReturnCodeKeys.E001;
  }


  /**
   * 
   * 服务节点注册
   *
   * 功能说明: 系统初始化后，服务节点可以通过注册接口注册到本系统。
   * 
   * 参数说明： nodeId 服务节点编号, 每个服务节点全局唯一的标识, 取值范围： 大于0；
   * 
   * 输出说明： 注册成功，返回E003:服务节点注册成功。 如果服务节点编号小于等于0, 返回E004:服务节点编号非法。 如果服务节点编号已注册, 返回E005:服务节点已注册。
   * 
   * @author dubx
   * @param nodeId
   * @return
   */
  public int registerNode(int nodeId) {
    if (nodeId > 0) {
      if (nodeList.isEmpty()) {
        nodeList.add(nodeId);
        Collections.sort(nodeList);
        return ReturnCodeKeys.E003;
      } else {
        if (nodeList.contains(nodeId)) {
          return ReturnCodeKeys.E005;
        } else {
          nodeList.add(nodeId);
          Collections.sort(nodeList);
          return ReturnCodeKeys.E003;
        }
      }
    } else {
      return ReturnCodeKeys.E004;
    }

  }

  /**
   * 服务节点注销
   * 
   * 功能说明: 1、从系统中删除服务节点； 2、如果该服务节点正运行任务，则将运行的任务移到任务挂起队列中，等待调度程序调度。
   * 
   * 参数说明： nodeId服务节点编号, 每个服务节点全局唯一的标识, 取值范围： 大于0。
   * 
   * 输出说明： 注销成功，返回E006:服务节点注销成功。 如果服务节点编号小于等于0, 返回E004:服务节点编号非法。 如果服务节点编号未被注册, 返回E007:服务节点不存在。
   *
   * @author dubx
   * @param nodeId
   * @return
   */
  public int unregisterNode(int nodeId) {
    if (nodeId > 0) {
      if (nodeList.isEmpty()) {
        return ReturnCodeKeys.E006;
      } else {
        if (!nodeList.contains(nodeId)) {
          return ReturnCodeKeys.E007;
        } else {
          nodeList.remove(new Integer(nodeId));
          return ReturnCodeKeys.E006;
        }
      }
    } else {
      return ReturnCodeKeys.E004;
    }
  }


  /**
   * 添加任务
   * 
   * 功能说明: 将新的任务加到系统的挂起队列中，等待服务调度程序来调度。
   * 
   * 参数说明： taskId任务编号；取值范围： 大于0。 consumption资源消耗率；
   * 
   * 输出说明： 添加成功，返回E008任务添加成功。 如果任务编号小于等于0, 返回E009:任务编号非法。 如果相同任务编号任务已经被添加, 返回E010:任务已添加。
   *
   * @author dubx
   * @param taskId
   * @param consumption
   * @return
   */
  public int addTask(int taskId, int consumption) {
    if(taskId > 0 && consumption > 0){     
      if (tasksList.contains(taskId)) {
        return ReturnCodeKeys.E010;
      }
      tasksList.add(taskId);
      tasksMap.put(taskId, consumption);
      Collections.sort(tasksList, comparatorTasksMap);
      return ReturnCodeKeys.E008;
    }else{
      return ReturnCodeKeys.E009;
    }
  }


  /**
   * 
   * 删除任务
   * 
   * 1.删除成功，返回E011:任务删除成功。
   * 
   * 2.如果任务编号小于等于0, 返回E009:任务编号非法。
   * 
   * 3.如果指定编号的任务未被添加, 返回E012:任务不存在。
   *
   * @author dubx
   * @param taskId
   * @return
   */
  public int deleteTask(int taskId) {
    if (taskId <= 0) {
      return ReturnCodeKeys.E009;
    }
    if (!tasksList.contains(taskId)) {
      return ReturnCodeKeys.E012;
    }
    tasksList.remove(new Integer(taskId));
    tasksMap.remove(new Integer(taskId));
    return ReturnCodeKeys.E011;
  }


  /**
   * 
   * 任务数量
   *
   * @author dubx
   * @param taskInfos
   * @return
   */
  private int getTasksCount(List<TaskInfo> taskInfos) {
    int result = 0;
    for (TaskInfo taskInfo : taskInfos) {
      result += tasksMap.get(taskInfo.getTaskId());
    }
    return result;
  }



  /**
   * 
   * 任务调度
   * 
   * 功能说明: 如果挂起队列中有任务存在，则进行根据上述的任务调度策略，获得最佳迁移方案，进行任务的迁移， 返回调度成功
   * 如果没有挂起的任务，则将运行中的任务则根据上述的任务调度策略，获得最佳迁移方案； 如果在最佳迁移方案中，任意两台不同服务节点上的任务资源总消耗率的差值小于等于调度阈值，
   * 则进行任务的迁移，返回调度成功， 如果在最佳迁移方案中，任意两台不同服务节点上的任务资源总消耗率的差值大于调度阈值，则不做任务的迁移，返回无合适迁移方案
   * 
   * 参数说明： threshold系统任务调度阈值，取值范围： 大于0；
   * 
   * 输出说明： 如果调度阈值取值错误，返回E002调度阈值非法。 如果获得最佳迁移方案, 进行了任务的迁移,返回E013: 任务调度成功;
   * 如果所有迁移方案中，总会有任意两台服务器的总消耗率差值大于阈值。则认为没有合适的迁移方案,返回 E014:无合适迁移方案;
   *
   * @author dubx
   * @param threshold
   * @return
   */
  public int scheduleTask(int threshold) {

    if (tasksList.isEmpty()) {
      return ReturnCodeKeys.E014;
    }
    
    if(threshold <= 0){
      return ReturnCodeKeys.E014;
    }

    boolean status = false;
    this.threshold = threshold;
    List<Integer> tmpTasks = new ArrayList<Integer>();
    for (Integer nodeId : nodeList) {
      List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();
      taskStatusMap.put(nodeId, taskInfos);
    }

    for (Integer taskId : tasksList) {
      tmpTasks.add(taskId);
    }
    
    //执行任务
    while (!status || tmpTasks.size() > 0) {
      for (Integer taskId : tmpTasks) {
        int nodeId = getNodeId();
        List<TaskInfo> taskInfos = taskStatusMap.get(nodeId);
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setNodeId(nodeId);
        taskInfo.setTaskId(taskId);
        taskInfos.add(taskInfo);
        tmpTasks.remove(new Integer(taskId));
        status = getStatus(nodeId);
        break;
      }
           
      if (tmpTasks.size() == 0 && !status)
        return ReturnCodeKeys.E014;
    }
    return ReturnCodeKeys.E013;
  }


  /**
   * 查询任务状态列表
   * 
   * 功能说明: 查询获得所有已添加任务的任务状态, 以任务列表方式返回。
   * 
   * 参数说明： Tasks 保存所有任务状态列表；要求按照任务编号升序排列, 如果该任务处于挂起队列中, 所属的服务编号为-1; 在保存查询结果之前,要求将列表清空. 输出说明： 未做此题返回
   * E000方法未实现。 如果查询结果参数tasks为null，返回E016:参数列表非法 如果查询成功, 返回E015: 查询任务状态成功;查询结果从参数Tasks返回。
   *
   * @author dubx
   * @param tasks
   * @return
   */
  public int queryTaskStatus(List<TaskInfo> tasks) {
    if (tasks == null) {
      return ReturnCodeKeys.E016;
    }
    for (Integer nodeId : taskStatusMap.keySet()) {
      tasks.addAll(taskStatusMap.get(nodeId));
    }
    Collections.sort(tasks, comparatorTask);
    return ReturnCodeKeys.E015;
  }
  
  
  /**
   * 
   * 判断是否超过阈值
   *
   * @author dubx
   * @param nodeId
   * @return
   */
  private boolean getStatus(int nodeId) {
    boolean balance = true;
    int count = getTasksCount(taskStatusMap.get(nodeId));
    for (Integer id : nodeList) {
      if (!id.equals(nodeId)) {
        int c = 0;
        if (taskStatusMap.get(id) == null) {
          c = 0;
        } else {
          c = getTasksCount(taskStatusMap.get(id));
        }
        if (Math.abs(c - count) > this.threshold)
          balance =  false;
      }
    }
    return balance;
  }
  
  /**
   * 
   * 获取节点id
   *
   * @author dubx
   * @return
   */
  private int getNodeId() {
    int nId = -1;
    int min = Integer.MAX_VALUE;
    for (Integer nodeId : nodeList) {
      List<TaskInfo> taskInfos = taskStatusMap.get(nodeId);
      if (taskInfos == null) {
        return nodeId;
      } else {
        int c = getTasksCount(taskInfos);
        if (c < min) {
          min = c;
          nId = nodeId;
        }
      }
    }
    return nId;
  }

}
