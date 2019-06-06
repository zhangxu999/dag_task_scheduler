# 任务放置 消息格式
``` json
{
  "task_id":123233232,
  "other_item":"ABCD",
  "commend_type": "exit,work",
}
```
# threadPod 回复 消息格式
 - data_loader
``` json
{
  "task_id":123454432,
  "node_id":765423,
  "result": "done/exception",
  ""
}
```
 - decision_maker
``` json
 {
  "task_id":123454432,
  "node_id":765423,
  "result": "success",
  "option": "1/2/3/exception"
  ""
}
```