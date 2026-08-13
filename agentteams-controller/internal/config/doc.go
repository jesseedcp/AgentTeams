// Package config 把环境变量和安装参数转换为 Controller 内部的强类型
// Config。它是部署界面与运行代码之间的边界：默认值、路径和
// provider 选择都应在这里集中解析，业务包不应到处重复读 os.Getenv。
// 这样同一份启动配置只有一种解释，测试也能构造确定的输入。
package config
