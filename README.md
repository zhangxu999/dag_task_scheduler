# dag_task_scheduler
基于有向无环图的任务处理编程框架, 采用DAG有向无环图的概念分解 任务复杂,前后依赖很多的数据处理任务. 采用redis作为任务存储调用队列, 任务可以分布式分解运行.框架解决了复杂任务的一下困难:
1. 数据依赖复杂 (通过将任务分解为基本节点,并允许基本任务自由组合,)
2. 编写困难, 可以很方便的配置执行编写基本任务单元的开发者无需对底层调度策略有过多了解,即可像搭积木一样组合基本任务,控制任务逻辑
3. 自动调度管理资源占用,通过不同的调度器,为不同的任务采用不同的调度策略.


运行方法:
1. 在本机启动redis.
2. 启动docker镜像.
docker 镜像:
```
docker pull zhangxudedocker/dag_task_scheduler:0.2
docker run -d -name task_scheduler --network="host" -v /<your_sqlite_path>/data:/data zhangxudedocker/dag_task_scheduler:0.2
```
注意修改run.sh 中graph.sqlite的地址,确保数据库文件能正确挂载.

运行superset 状态监控可视化web页面.
     
为了方便监控任务分布图中任务队列的长度,我们采用superset 开发监控的dashboard.界面效果如下:
![image](https://github.com/zhangxu999/dag_task_scheduler/blob/master/design/dash.png)
可以查看任务队列长度, 任务关系拓扑图结构.

superset web界面docker镜像运行方法
```
docker pull zhangxudedocker/supersetdash:0.1
docker run -d -name dash --network="host" -v /<your_sqlite_path>/graph.sqlite:/home/superset/graph.sqlite/graph.sqlite -p 8088:8088 zhangxudedocker/supersetdash:0.1
```
