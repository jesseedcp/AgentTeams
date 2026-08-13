package controller

import (
	"context"
	"time"

	v1beta1 "github.com/agentscope-ai/AgentTeams/agentteams-controller/api/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const defaultAutoSleepInterval = time.Minute

type AutoSleepController struct {
	client.Client
	Namespace string
	Interval  time.Duration
	Now       func() time.Time
}

func (c *AutoSleepController) Start(ctx context.Context) error {
	// 逻辑说明：Start 接收 ctx(context.Context)，依次借助 reconcile、NewTicker、Stop、Done启动Worker 自动休眠状态的期望结果。
	// 返回/状态：返回 error；会更新 Worker 自动休眠状态的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	interval := c.Interval
	if interval <= 0 {
		interval = defaultAutoSleepInterval
	}
	if c.Now == nil {
		c.Now = time.Now
	}

	c.reconcile(ctx)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.reconcile(ctx)
		}
	}
}

func (c *AutoSleepController) reconcile(ctx context.Context) {
	// 逻辑说明：reconcile 接收 ctx(context.Context)，依次借助 List、InNamespace、shouldSleep、Now调谐Worker 自动休眠状态的期望结果。
	// 返回/状态：返回 无；会调用下层服务修改外部资源，并把阶段、条件与已应用版本写回 CR status。
	// 失败/重试：error 或 RequeueAfter 交给 controller-runtime；重复执行必须把同一 spec 收敛到同一状态。
	logger := log.FromContext(ctx).WithName("auto-sleep")

	var workers v1beta1.WorkerList
	if err := c.List(ctx, &workers, client.InNamespace(c.Namespace)); err != nil {
		logger.Error(err, "list workers")
		return
	}
	for _, worker := range workers.Items {
		if !shouldSleep(c.Now(), worker.Spec.DesiredState(), worker.Spec.IdleTimeout, worker.Status.LastActiveAt) {
			continue
		}
		if err := c.setWorkerState(ctx, worker.Name, "Sleeping"); err != nil {
			logger.Error(err, "set worker sleeping", "worker", worker.Name)
		}
	}

	// Team members are Worker CRs referenced from spec.workerMembers, so the
	// Worker loop above covers both standalone and team-bound workers.
}

func shouldSleep(now time.Time, state, idleTimeout, lastActiveAt string) bool {
	// 逻辑说明：shouldSleep 接收 now(time.Time)、state/idleTimeout/lastActiveAt(string)，依次借助 ParseDuration、Parse、Sub判定Worker 自动休眠状态的期望结果。
	// 返回/状态：返回 bool；仅在内存中整理或比较输入，不读取或写入外部系统。
	// 失败/重试：纯计算不自行重试；若函数返回错误，调用者应修正输入或把错误交给上层调谐。
	if state != "Running" || idleTimeout == "" || lastActiveAt == "" {
		return false
	}
	timeout, err := time.ParseDuration(idleTimeout)
	if err != nil || timeout <= 0 {
		return false
	}
	lastActive, err := time.Parse(time.RFC3339, lastActiveAt)
	if err != nil {
		return false
	}
	return now.Sub(lastActive) > timeout
}

func (c *AutoSleepController) setWorkerState(ctx context.Context, name, state string) error {
	// 逻辑说明：setWorkerState 接收 ctx(context.Context)、name/state(string)，依次借助 RetryOnConflict、Get、IgnoreNotFound、DesiredState设置Worker 自动休眠状态的期望结果。
	// 返回/状态：返回 error；会更新 Worker 自动休眠状态的内存状态，存在客户端调用时还可能同步相应外部资源。
	// 失败/重试：输入或依赖调用失败会返回错误；是否重排由上层调谐器决定，本函数不隐藏失败。
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		var worker v1beta1.Worker
		if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: c.Namespace}, &worker); err != nil {
			return client.IgnoreNotFound(err)
		}
		if worker.Spec.DesiredState() == state {
			return nil
		}
		worker.Spec.State = &state
		return c.Update(ctx, &worker)
	})
}
