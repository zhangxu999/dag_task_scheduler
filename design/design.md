#  失败数据保证
###  队列(mq,redis)的可替换性
   - 使用rabbitMQ
   - 使用 redis
      - 并发连接



#  局部超时异常机制
   - 总体超时退出机制
   - operator 失败重试机制


#  心跳检测
#  进程管理保证
   1. 资源是有限的, 要保证不要有过多的无用进程驻留,也就是说进程线程要有优雅的退出机制
   2. 各个节点 node 是处理数据速度是不一样的, 需要的worker数量不一样.
   3.  并发的下游执行
#  规则动态修改&修改
# web 状态页面
# web 规则页面
# 线程代码热更新


