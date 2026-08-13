"""AgentScope Manager test suite."""

# 初学者可按速度和真实性理解各层：unit 隔离单个组件，integration 连接多个真实模块，
# contract 防止与 Matrix/Skill/上游资源约定漂移，fault_injection 主动制造超时与重启，
# e2e 则需要显式启用并连接真实 Kubernetes。低层通过不代表高层一定通过。
