// Package matrix 封装 Matrix homeserver 和 AppService API。Controller 通过它创建
// Agent/Human 账号、房间、别名和成员关系，但对话历史和房间事件仍以
// Matrix 为权威来源。
//
// Matrix 请求可能超时、被重试或在回答丢失前已成功，所以“确保成员
// 存在”类操作要先查看当前 membership，已满足时直接成功。稳定房间别名
// 必须与当前真实 room ID 保持一致，否则前端会进入已经失效的历史房间。
package matrix
