"""Protocol contract tests."""

# 这里锁定 Matrix 事件形状、Manager Skill、资源字段和上游基线等跨组件约定。
# 契约测试不问实现内部怎么写，只要求双方可观察接口继续兼容；失败通常意味着需同步更新多个消费者。
