{{/*
Infrastructure abstraction helpers.

初学者可以把这些 helper 理解成“选择内部地址的路由表”：managed 模式返回本
Chart 创建的 Service DNS；existing 模式返回用户提供的外部地址。Controller 和
Agent 使用内部地址通信，浏览器才使用 publicURL，避免把宿主机端口误塞进 Pod。

Phase 2 reads the new public values API (`matrix.*`, `gateway.*`, `storage.*`).
The Higress dependency still consumes top-level `higress:` values because the
dependency name remains `higress`; `gateway.higress.enabled` is only the
materialized condition flag.
*/}}

{{- define "agentteams.matrix.internalURL" -}}
{{- if and (eq .Values.matrix.provider "tuwunel") (eq .Values.matrix.mode "managed") -}}
{{- include "agentteams.tuwunel.internalURL" . -}}
{{- else -}}
{{- .Values.matrix.internalURL | default "" -}}
{{- end -}}
{{- end }}

{{- define "agentteams.matrix.serverName" -}}
{{- if and (eq .Values.matrix.provider "tuwunel") (eq .Values.matrix.mode "managed") -}}
{{- include "agentteams.tuwunel.serverName" . -}}
{{- else -}}
{{- .Values.matrix.serverName | default "" -}}
{{- end -}}
{{- end }}

{{- define "agentteams.gateway.publicURL" -}}
{{- required "gateway.publicURL is required" .Values.gateway.publicURL -}}
{{- end }}

{{- define "agentteams.gateway.internalURL" -}}
{{- if and (eq .Values.gateway.provider "higress") (eq .Values.gateway.mode "managed") -}}
{{- include "agentteams.higress.gatewayURL" . -}}
{{- else if eq .Values.gateway.provider "ai-gateway" -}}
{{/* External APIG: workers reach the gateway via its public URL. */ -}}
{{- include "agentteams.gateway.publicURL" . -}}
{{- else -}}
{{- fail (printf "unsupported gateway combination %s/%s" .Values.gateway.provider .Values.gateway.mode) -}}
{{- end -}}
{{- end }}

{{- define "agentteams.gateway.adminURL" -}}
{{- if and (eq .Values.gateway.provider "higress") (eq .Values.gateway.mode "managed") -}}
{{- include "agentteams.higress.consoleURL" . -}}
{{- else if eq .Values.gateway.provider "ai-gateway" -}}
{{/* APIG does not expose a console URL from within the cluster: the
     controller talks to it via the regional Aliyun OpenAPI endpoint, so
     no admin URL is meaningful here. Leave empty to mean "unset" and let
     callers decide whether to guard emission of AGENTTEAMS_AI_GATEWAY_ADMIN_URL. */ -}}
{{- else -}}
{{- fail (printf "unsupported gateway admin combination %s/%s" .Values.gateway.provider .Values.gateway.mode) -}}
{{- end -}}
{{- end }}

{{- define "agentteams.gateway.higress.enabled" -}}
{{- if and (eq .Values.gateway.provider "higress") (eq .Values.gateway.mode "managed") -}}true{{- else -}}false{{- end -}}
{{- end }}

{{- define "agentteams.storage.endpoint" -}}
{{- if and (eq .Values.storage.provider "minio") (eq .Values.storage.mode "managed") -}}
{{- include "agentteams.minio.internalURL" . -}}
{{- else if eq .Values.storage.provider "oss" -}}
{{/* External OSS: the authoritative endpoint is returned by the
     credential-provider sidecar alongside each STS token. If the chart
     user supplies an override (storage.oss.endpoint) we honour it so that
     worker scripts can hard-code mc hosts when the provider isn't
     reachable from the worker network (rare). */ -}}
{{- .Values.storage.oss.endpoint | default "" -}}
{{- else -}}
{{- fail (printf "unsupported storage combination %s/%s" .Values.storage.provider .Values.storage.mode) -}}
{{- end -}}
{{- end }}

{{- define "agentteams.storage.bucket" -}}
{{- required "storage.bucket is required" .Values.storage.bucket -}}
{{- end }}

{{- define "agentteams.storage.remoteRoot" -}}
{{- printf "agentteams/%s" (include "agentteams.storage.bucket" .) -}}
{{- end }}

{{- define "agentteams.storage.adminSecretName" -}}
{{- if and (eq .Values.storage.provider "minio") (eq .Values.storage.mode "managed") -}}
{{- include "agentteams.minio.fullname" . -}}
{{- end -}}
{{- end }}

{{- define "agentteams.storage.adminAccessKeyKey" -}}
{{- if and (eq .Values.storage.provider "minio") (eq .Values.storage.mode "managed") -}}MINIO_ROOT_USER{{- end -}}
{{- end }}

{{- define "agentteams.storage.adminSecretKeyKey" -}}
{{- if and (eq .Values.storage.provider "minio") (eq .Values.storage.mode "managed") -}}MINIO_ROOT_PASSWORD{{- end -}}
{{- end }}

{{- define "agentteams.manager.spec" -}}
{{- $spec := dict
  "model" (.Values.manager.model | default .Values.credentials.defaultModel)
  "runtime" (.Values.manager.runtime | default "agentscope")
  "image" (include "agentteams.manager.image" .)
  "resources" .Values.manager.resources
  "codingCLI" (dict
    "enabled" (.Values.manager.codingCLI.enabled | default false)
    "providers" (.Values.manager.codingCLI.providers | default (list))
    "hostPath" (.Values.manager.codingCLI.hostPath | default "")
    "mountPath" (.Values.manager.codingCLI.mountPath | default "/opt/agentteams/coding-cli")
    "trustedDirectory" (.Values.manager.codingCLI.trustedDirectory | default "/opt/agentteams/coding-cli/bin")
    "timeoutSeconds" (.Values.manager.codingCLI.timeoutSeconds | default 600)
    "maxOutputBytes" (.Values.manager.codingCLI.maxOutputBytes | default 65536)
  )
-}}
{{- $spec | toJson -}}
{{- end }}
