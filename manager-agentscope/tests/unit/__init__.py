"""Unit tests."""

# 单元测试通过 fake/mock 隔离网络、进程和存储，只验证一个组件的输入、状态变化与输出。
# fake 能让失败可重复，但不能证明真实 Matrix、S3 或 Controller 已正确部署；该责任属于集成与 E2E 层。
