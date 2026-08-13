// Package fixtures 构造测试中反复使用的 Worker、Team 和 Manager 样本 CR。
// fixture 只提供与测试目标无关的稳定默认值，每个测试再显式修改
// 它要验证的字段。这样失败时能看出是哪个期望状态差异，而不是
// 被大量重复样板掩盖。fixture 不是生产默认配置的权威来源。
package fixtures
