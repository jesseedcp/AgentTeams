#!/bin/sh
# agentteams-find-skill.sh - Unified skill discovery wrapper for Workers
# Backends:
#   - skills_sh: delegate to `skills find`
#   - nacos: query local/default Nacos CLI profile and render skills-style output

set -eu

MAX_RESULTS="${AGENTTEAMS_FIND_SKILL_MAX_RESULTS:-6}"
PAGE_SIZE="${AGENTTEAMS_FIND_SKILL_NACOS_PAGE_SIZE:-100}"

RESET='[0m'
DIM='[38;5;102m'
TEXT='[38;5;145m'

get_script_path() {
    # 逻辑说明：把 `$0` 解析为尽可能稳定的绝对脚本路径，供 usage 输出可复制命令；只读取 PATH 和目录，不修改文件。
    raw_path="${0:-agentteams-find-skill.sh}"

    case "${raw_path}" in
        /*)
            printf '%s\n' "${raw_path}"
            ;;
        */*)
            dir_path="$(cd "$(dirname "${raw_path}")" && pwd -P)"
            printf '%s/%s\n' "${dir_path}" "$(basename "${raw_path}")"
            ;;
        *)
            resolved_path="$(command -v "${raw_path}" 2>/dev/null || true)"
            if [ -n "${resolved_path}" ]; then
                case "${resolved_path}" in
                    /*)
                        printf '%s\n' "${resolved_path}"
                        ;;
                    *)
                        dir_path="$(cd "$(dirname "${resolved_path}")" && pwd -P)"
                        printf '%s/%s\n' "${dir_path}" "$(basename "${resolved_path}")"
                        ;;
                esac
            else
                printf '%s\n' "${raw_path}"
            fi
            ;;
    esac
}

SCRIPT_PATH="$(get_script_path)"

usage() {
    # 逻辑说明：把 find/install 用法和后端环境变量写到标准输出；这里只展示帮助，不探测注册中心或执行安装。
    cat <<EOF
Usage:
  ${SCRIPT_PATH} find <query>
  ${SCRIPT_PATH} install <skill>

Environment:
  SKILLS_API_URL=https://skills.sh            Use skills.sh backend
  SKILLS_API_URL=nacos://host:port            Use Nacos backend (default)
EOF
}

get_skills_api_url() {
    # 逻辑说明：按 `SKILLS_API_URL`、`AGENTTEAMS_SKILLS_API_URL`、skills.sh 默认值的优先级返回注册中心地址，不改变环境变量。
    if [ -n "${SKILLS_API_URL:-}" ]; then
        printf '%s\n' "${SKILLS_API_URL}"
        return
    fi
    if [ -n "${AGENTTEAMS_SKILLS_API_URL:-}" ]; then
        printf '%s\n' "${AGENTTEAMS_SKILLS_API_URL}"
        return
    fi
    printf '%s\n' "https://skills.sh"
}

resolve_controller_bearer() {
    # 逻辑说明：按直接 token、token 文件、Worker API key 的优先级输出 Controller bearer；文件不存在时不伪造凭据，且不会把 token 写入日志。
    if [ -n "${AGENTTEAMS_AUTH_TOKEN:-}" ]; then
        printf '%s' "${AGENTTEAMS_AUTH_TOKEN}"
        return
    fi
    if [ -n "${AGENTTEAMS_AUTH_TOKEN_FILE:-}" ] && [ -f "${AGENTTEAMS_AUTH_TOKEN_FILE}" ]; then
        cat "${AGENTTEAMS_AUTH_TOKEN_FILE}"
        return
    fi
    if [ -n "${AGENTTEAMS_WORKER_API_KEY:-}" ]; then
        printf '%s' "${AGENTTEAMS_WORKER_API_KEY}"
    fi
}

ensure_nacos_sts_credentials() {
    # 逻辑说明：已有 STS access key 时直接复用，否则带 bearer 调 Controller 换取临时 Nacos 凭据并导出到当前进程；HTTP/JSON 校验失败立即退出，避免用空凭据访问注册中心。
    [ -n "${AGENTTEAMS_NACOS_STS_ACCESS_KEY:-}" ] && return 0

    controller_url="${AGENTTEAMS_CONTROLLER_URL:-}"
    bearer="$(resolve_controller_bearer)"
    if [ -z "${controller_url}" ] || [ -z "${bearer}" ]; then
        echo "error: NACOS_AUTH_TYPE=sts-agentteams requires AGENTTEAMS_CONTROLLER_URL and a controller bearer token" >&2
        exit 1
    fi

    resp="$(curl -s -w "\n%{http_code}" -X POST "${controller_url%/}/api/v1/credentials/sts" \
        -H "Authorization: Bearer ${bearer}" \
        --connect-timeout 10 --max-time 30 2>&1)"
    http_code="$(printf '%s\n' "${resp}" | tail -1)"
    body="$(printf '%s\n' "${resp}" | sed '$d')"
    if [ "${http_code}" != "200" ]; then
        echo "error: controller STS request for AI registry failed (HTTP ${http_code})" >&2
        printf '%s\n' "${body}" >&2
        exit 1
    fi

    AGENTTEAMS_NACOS_STS_ACCESS_KEY="$(printf '%s\n' "${body}" | jq -r '.access_key_id')"
    AGENTTEAMS_NACOS_STS_SECRET_KEY="$(printf '%s\n' "${body}" | jq -r '.access_key_secret')"
    AGENTTEAMS_NACOS_STS_SECURITY_TOKEN="$(printf '%s\n' "${body}" | jq -r '.security_token')"
    if [ -z "${AGENTTEAMS_NACOS_STS_ACCESS_KEY}" ] || [ "${AGENTTEAMS_NACOS_STS_ACCESS_KEY}" = "null" ]; then
        echo "error: failed to parse AI registry STS credentials from controller response" >&2
        exit 1
    fi
}

get_registry_label() {
    # 逻辑说明：根据 backend 和实际连接参数生成人类可读的注册中心标签；Nacos 地址解析失败时退回通用标签，不发起网络请求。
    backend="$1"
    api_url="$(get_skills_api_url)"
    case "${backend}" in
        nacos)
            connection="$(
                derive_nacos_connection
            )"
            host="$(printf '%s\n' "${connection}" | sed -n '1p')"
            port="$(printf '%s\n' "${connection}" | sed -n '2p')"
            if [ -n "${host}" ]; then
                printf '%s\n' "Nacos (nacos://${host}:${port})"
            else
                printf '%s\n' "Nacos"
            fi
            ;;
        skills_sh)
            printf '%s\n' "skills.sh (${api_url})"
            ;;
        *)
            printf '%s\n' "${backend}"
            ;;
    esac
}

detect_backend() {
    # 逻辑说明：读取最终 API URL，以 `nacos://` 选择 Nacos，以 HTTP(S) 或未知格式选择 skills.sh，并把规范化 backend 名写到标准输出。
    api_url="$(get_skills_api_url)"
    case "${api_url}" in
        nacos://*) printf '%s\n' "nacos" ;;
        http://*|https://*) printf '%s\n' "skills_sh" ;;
        *) printf '%s\n' "skills_sh" ;;
    esac
}

run_skills_find() {
    # 逻辑说明：把查询参数原样交给 `skills find`，成功后在结果前补注册中心标签；子命令失败时保留其退出码和错误语义。
    output="$(skills find "$@")" || exit $?
    printf 'Registry: %s\n\n' "$(get_registry_label "skills_sh")"
    printf '%s\n' "${output}"
}

run_skills_install() {
    # 逻辑说明：校验至少提供一个 skill 名，然后用 `exec skills add -g -y` 替换当前进程执行全局安装；缺少名称时在产生安装副作用前退出。
    if [ $# -lt 1 ]; then
        echo "error: skill name is required for install" >&2
        exit 1
    fi
    exec skills add "$1" -g -y
}

run_nacos_install() {
    # 逻辑说明：校验 skill 名后调用统一 Nacos CLI 的 `skill-get` 下载/安装；连接、认证和 CLI 错误由 `run_nacos_cli` 原样传播。
    if [ $# -lt 1 ]; then
        echo "error: skill name is required for install" >&2
        exit 1
    fi
    run_nacos_cli skill-get "$1"
}

derive_nacos_connection() {
    # 逻辑说明：合并 `nacos://` URL 与显式环境变量，解析 host、port、namespace、用户名、密码和 token，并按固定六行输出；显式环境变量优先，函数本身不连接网络。
    api_url="$(get_skills_api_url)"
    host="${AGENTTEAMS_NACOS_HOST:-}"
    port="${AGENTTEAMS_NACOS_PORT:-}"
    namespace=""
    username="${AGENTTEAMS_NACOS_USERNAME:-}"
    password="${AGENTTEAMS_NACOS_PASSWORD:-}"
    token="${AGENTTEAMS_NACOS_TOKEN:-}"

    if [ -n "${api_url}" ] && [ "${api_url#nacos://}" != "${api_url}" ]; then
        api_url="${api_url#nacos://}"

        auth_part="${api_url%%@*}"
        if [ "${auth_part}" != "${api_url}" ]; then
            if [ -z "${username}" ]; then
                username="${auth_part%%:*}"
            fi
            if [ -z "${password}" ]; then
                auth_rest="${auth_part#*:}"
                if [ "${auth_rest}" != "${auth_part}" ]; then
                    password="${auth_rest}"
                fi
            fi
            api_url="${api_url#*@}"
        fi

        query_part=""
        if [ "${api_url#*\?}" != "${api_url}" ]; then
            query_part="${api_url#*\?}"
            api_url="${api_url%%\?*}"
        fi

        if [ "${api_url#*/}" != "${api_url}" ]; then
            path_part="${api_url#*/}"
            api_url="${api_url%%/*}"
            if [ -z "${namespace}" ] && [ -n "${path_part}" ]; then
                namespace="${path_part%%/*}"
            fi
        fi

        if [ -z "${host}" ]; then
            host="${api_url%%:*}"
        fi
        if [ -z "${port}" ] && [ "${api_url#*:}" != "${api_url}" ]; then
            port="${api_url##*:}"
        fi
        if [ -z "${port}" ]; then
            port="8848"
        fi

        if [ -z "${namespace}" ] && [ -n "${query_part}" ]; then
            namespace="$(printf '%s\n' "${query_part}" | sed -n 's/.*[?&]namespace=\([^&]*\).*/\1/p')"
            [ -n "${namespace}" ] || namespace="$(printf '%s\n' "${query_part}" | sed -n 's/^namespace=\([^&]*\).*$/\1/p')"
        fi
    fi

    printf '%s\n%s\n%s\n%s\n%s\n%s\n' \
        "${host}" "${port}" "${namespace}" "${username}" "${password}" "${token}"
}

run_nacos_cli() {
    # 逻辑说明：读取六项 Nacos 连接参数，组装 `npx @nacos-group/cli` 命令；STS 模式先换取临时凭据，其他模式附加用户认证，最后执行且保留真实退出码。
    if [ $# -lt 1 ]; then
        return 0
    fi

    connection="$(
        derive_nacos_connection
    )"
    host="$(printf '%s\n' "${connection}" | sed -n '1p')"
    port="$(printf '%s\n' "${connection}" | sed -n '2p')"
    namespace="$(printf '%s\n' "${connection}" | sed -n '3p')"
    username="$(printf '%s\n' "${connection}" | sed -n '4p')"
    password="$(printf '%s\n' "${connection}" | sed -n '5p')"
    token="$(printf '%s\n' "${connection}" | sed -n '6p')"

    set -- npx -y @nacos-group/cli "$@"
    [ -n "${host}" ] && set -- "$@" --host "${host}"
    [ -n "${port}" ] && set -- "$@" --port "${port}"
    [ -n "${namespace}" ] && set -- "$@" --namespace "${namespace}"
    if [ "${NACOS_AUTH_TYPE:-}" = "sts-agentteams" ]; then
        ensure_nacos_sts_credentials
        set -- "$@" --auth-type sts-agentteams \
            --access-key "${AGENTTEAMS_NACOS_STS_ACCESS_KEY}" \
            --secret-key "${AGENTTEAMS_NACOS_STS_SECRET_KEY}" \
            --security-token "${AGENTTEAMS_NACOS_STS_SECURITY_TOKEN}"
    else
        [ -n "${username}" ] && set -- "$@" --username "${username}"
        [ -n "${password}" ] && set -- "$@" --password "${password}"
        [ -n "${token}" ] && set -- "$@" --token "${token}"
    fi
    "$@"
}

append_skill_lines() {
    # 逻辑说明：从一页 Nacos CLI 文本中提取编号条目，附加 pattern/page/item 排名列后写入候选 TSV；非结果行被忽略，供后续去重评分。
    output="$1"
    pattern_rank="$2"
    page="$3"
    out_file="$4"

    printf '%s\n' "${output}" | awk -v pattern_rank="${pattern_rank}" -v page="${page}" '
        /^[[:space:]]*[0-9]+\.[[:space:]]/ {
            line = $0
            sub(/^[[:space:]]*[0-9]+\.[[:space:]]+/, "", line)
            item_rank += 1
            printf "%s\t%s\t%s\t%s\n", pattern_rank, page, item_rank, line
        }
    ' >> "${out_file}"
}

fetch_nacos_pattern() {
    # 逻辑说明：对单个搜索 pattern 分页调用 `skill-list`，逐页追加候选，直到空页或不足 PAGE_SIZE；任一页失败就输出错误并停止，避免用不完整结果冒充完整搜索。
    pattern="$1"
    pattern_rank="$2"
    out_file="$3"

    page=1
    while :; do
        page_output="$(run_nacos_cli skill-list --name "${pattern}" --page "${page}" --size "${PAGE_SIZE}" 2>&1)" || {
            printf '%s\n' "${page_output}" >&2
            exit 1
        }

        page_count="$(printf '%s\n' "${page_output}" | awk '/^[[:space:]]*[0-9]+\.[[:space:]]/ { c++ } END { print c + 0 }')"
        [ "${page_count}" -gt 0 ] || break

        append_skill_lines "${page_output}" "${pattern_rank}" "${page}" "${out_file}"

        [ "${page_count}" -lt "${PAGE_SIZE}" ] && break
        page=$((page + 1))
    done
}

build_nacos_patterns() {
    # 逻辑说明：把查询统一为小写字母数字词组，同时生成完整短语和长度至少二的去重 token，写入 pattern 文件以扩大 Nacos 模糊检索召回率。
    query="$1"
    out_file="$2"

    printf '%s\n' "${query}" | awk '
        function add(pattern) {
            if (pattern == "" || seen[pattern]++) return
            print pattern
        }
        {
            q = tolower($0)
            gsub(/[^[:alnum:]]+/, " ", q)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", q)
            gsub(/[[:space:]]+/, " ", q)

            token_count = split(q, tokens, /[[:space:]]+/)
            add(q)
            for (i = 1; i <= token_count; i++) {
                token = tokens[i]
                if (length(token) < 2) continue
                add(token)
            }
        }
    ' > "${out_file}"
}

score_nacos_candidates() {
    # 逻辑说明：解析候选 TSV，按名称/描述的精确短语、子串、token 与前缀命中计算分数，再按名称和描述去重保留最高分，输出可排序结果；不会修改注册中心数据。
    query="$1"
    candidates_file="$2"
    scored_file="$3"

    awk -F '\t' -v query="${query}" '
        function trim(s) {
            sub(/^[[:space:]]+/, "", s)
            sub(/[[:space:]]+$/, "", s)
            return s
        }
        function normalize(s) {
            s = tolower(s)
            gsub(/[^[:alnum:]]+/, " ", s)
            gsub(/^[[:space:]]+|[[:space:]]+$/, "", s)
            gsub(/[[:space:]]+/, " ", s)
            return s
        }
        function has_word(words, token) {
            return index(" " words " ", " " token " ") > 0
        }
        function has_prefix(words, token,    count, i, arr) {
            count = split(words, arr, /[[:space:]]+/)
            for (i = 1; i <= count; i++) {
                if (arr[i] == "") continue
                if (index(arr[i], token) == 1) return 1
            }
            return 0
        }
        BEGIN {
            q_raw = tolower(query)
            q_words = normalize(query)
            token_count = split(q_words, raw_tokens, /[[:space:]]+/)
            effective_tokens = 0
            for (i = 1; i <= token_count; i++) {
                token = raw_tokens[i]
                if (token == "" || seen_query_token[token]++) continue
                effective_tokens += 1
                tokens[effective_tokens] = token
            }
        }
        NF >= 4 {
            pattern_rank = ($1 + 0)
            page = ($2 + 0)
            item_rank = ($3 + 0)
            line = $4

            name = line
            desc = ""
            split_pos = index(line, " - ")
            if (split_pos > 0) {
                name = substr(line, 1, split_pos - 1)
                desc = substr(line, split_pos + 3)
            }

            name = trim(name)
            desc = trim(desc)
            lname = tolower(name)
            ldesc = tolower(desc)
            name_words = normalize(name)
            desc_words = normalize(desc)

            exact_name_phrase = (q_words != "" && has_word(name_words, q_words))
            exact_desc_phrase = (q_words != "" && has_word(desc_words, q_words))
            raw_name_substring = (q_raw != "" && index(lname, q_raw) > 0)
            raw_desc_substring = (q_raw != "" && index(ldesc, q_raw) > 0)

            score = 0
            coverage = 0
            missing = 0

            if (lname == q_raw) score += 30000
            if (name_words == q_words && q_words != "") score += 24000
            if (exact_name_phrase) score += 12000
            if (exact_desc_phrase) score += 3000
            if (raw_name_substring) score += 1500
            if (raw_desc_substring) score += 400

            for (i = 1; i <= effective_tokens; i++) {
                token = tokens[i]
                matched = 0

                if (has_word(name_words, token)) {
                    score += 1800
                    matched = 1
                } else if (length(token) >= 4 && has_prefix(name_words, token)) {
                    score += 900
                    matched = 1
                } else if (has_word(desc_words, token)) {
                    score += 600
                    matched = 1
                } else if (length(token) >= 4 && has_prefix(desc_words, token)) {
                    score += 250
                    matched = 1
                }

                if (matched) coverage += 1
                else missing += 1
            }

            if (coverage == 0 && !exact_name_phrase && !exact_desc_phrase &&
                !raw_name_substring && !raw_desc_substring) {
                next
            }

            score += coverage * coverage * 500
            score -= missing * 200

            backend_bonus = 200 - (pattern_rank * 20) - ((page - 1) * 5) - item_rank
            if (backend_bonus > 0) score += backend_bonus

            key = lname SUBSEP ldesc
            if (!(key in best_score) || score > best_score[key]) {
                best_score[key] = score
                best_name[key] = name
                best_desc[key] = desc
            }
        }
        END {
            for (key in best_score) {
                printf "%08d\t%s\t%s\n", best_score[key], best_name[key], best_desc[key]
            }
        }
    ' "${candidates_file}" > "${scored_file}"
}

run_nacos_find() {
    # 逻辑说明：为一次查询创建临时目录，生成 patterns、分页收集候选、评分排序并展示前 MAX_RESULTS 条；trap 负责退出时清理临时文件，无结果时正常返回而不是安装任何内容。
    if [ $# -lt 1 ]; then
        printf '%sTip:%s search with %s%s find <query>%s\n' "${DIM}" "${RESET}" "${TEXT}" "${SCRIPT_PATH}" "${RESET}"
        exit 0
    fi

    query="$*"
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "${tmp_dir}"' EXIT INT TERM
    patterns="${tmp_dir}/patterns.txt"
    candidates="${tmp_dir}/candidates.tsv"
    scored="${tmp_dir}/scored.txt"
    sorted="${tmp_dir}/sorted.txt"

    build_nacos_patterns "${query}" "${patterns}"
    : > "${candidates}"

    pattern_rank=0
    while IFS= read -r pattern; do
        [ -n "${pattern}" ] || continue
        pattern_rank=$((pattern_rank + 1))
        fetch_nacos_pattern "${pattern}" "${pattern_rank}" "${candidates}"
    done < "${patterns}"

    score_nacos_candidates "${query}" "${candidates}" "${scored}"
    sort -r "${scored}" > "${sorted}"

    if [ ! -s "${sorted}" ]; then
        printf '%sNo skills found for "%s"%s\n' "${DIM}" "${query}" "${RESET}"
        exit 0
    fi

    first_name="$(awk -F '\t' 'NR == 1 { print $2; exit }' "${sorted}")"

    printf '%sRegistry:%s %s\n\n' "${DIM}" "${RESET}" "$(get_registry_label "nacos")"
    printf '%sInstall with%s %s%s install %s%s\n\n' \
        "${DIM}" "${RESET}" "${TEXT}" "${SCRIPT_PATH}" "${first_name}" "${RESET}"

    sed -n "1,${MAX_RESULTS}p" "${sorted}" | while IFS="$(printf '\t')" read -r score name desc; do
        if [ -z "${desc}" ]; then
            desc="Available from Nacos skill registry"
        fi

        printf '%s%s%s\n' "${TEXT}" "${name}" "${RESET}"
        printf '%s└ %s%s\n\n' "${DIM}" "${desc}" "${RESET}"
    done
}

command_name="${1:-find}"
if [ $# -gt 0 ]; then
    shift
fi

case "${command_name}" in
    find)
        backend="$(detect_backend)"
        case "${backend}" in
            skills_sh) run_skills_find "$@" ;;
            nacos) run_nacos_find "$@" ;;
            *)
                echo "error: unsupported skill backend: ${backend}" >&2
                exit 1
                ;;
        esac
        ;;
    install|get)
        backend="$(detect_backend)"
        case "${backend}" in
            skills_sh) run_skills_install "$@" ;;
            nacos) run_nacos_install "$@" ;;
            *)
                echo "error: unsupported skill backend: ${backend}" >&2
                exit 1
                ;;
        esac
        ;;
    help|-h|--help)
        usage
        ;;
    *)
        echo "error: unknown command: ${command_name}" >&2
        usage >&2
        exit 1
        ;;
esac
