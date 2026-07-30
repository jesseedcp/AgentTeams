ARG PYTHON_IMAGE=python:3.11-slim
ARG AGENTTEAMS_CONTROLLER_IMAGE=agentteams/agentteams-controller:latest

FROM ${PYTHON_IMAGE} AS python-builder

ARG APT_MIRROR=
ARG PIP_INDEX_URL=https://pypi.org/simple/

ENV VIRTUAL_ENV=/opt/agentteams/venv
ENV PATH="${VIRTUAL_ENV}/bin:${PATH}"

RUN if [ -n "${APT_MIRROR}" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g" \
            /etc/apt/sources.list.d/debian.sources; \
    fi \
    && apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=60 update \
    && apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=60 \
        install -y --no-install-recommends \
        gcc \
    && rm -rf /var/lib/apt/lists/* \
    && python -m venv "${VIRTUAL_ENV}" \
    && pip install --no-cache-dir --upgrade pip setuptools wheel

COPY manager-agentscope/pyproject.toml /build/manager-agentscope/
COPY manager-agentscope/src/ /build/manager-agentscope/src/

RUN pip install --no-cache-dir /build/manager-agentscope

FROM ${AGENTTEAMS_CONTROLLER_IMAGE} AS controller

FROM ${PYTHON_IMAGE}

ARG APT_MIRROR=

RUN if [ -n "${APT_MIRROR}" ]; then \
        sed -i "s|deb.debian.org|${APT_MIRROR}|g" \
            /etc/apt/sources.list.d/debian.sources; \
    fi \
    && apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=60 update \
    && apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=60 \
        install -y --no-install-recommends \
        ca-certificates \
        git \
        openssh-client \
        tini \
    && rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/agentteams/venv/bin:${PATH}"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV AGENTTEAMS_MANAGER_WORKSPACE=/var/lib/agentteams-manager
ENV AGENTTEAMS_MANAGER_HEALTH_PORT=18799

COPY --from=python-builder /opt/agentteams/venv/ /opt/agentteams/venv/
COPY --from=controller /usr/local/bin/agt /usr/local/bin/agt

COPY manager/agent/SOUL.md /opt/agentteams/manager/SOUL.md
COPY manager/agent/AGENTS.md /opt/agentteams/manager/AGENTS.md
COPY manager/agent/TOOLS.md /opt/agentteams/manager/TOOLS.md
COPY manager/agent/HEARTBEAT.md /opt/agentteams/manager/HEARTBEAT.md
COPY manager/agent/skills/ /opt/agentteams/manager/skills/
COPY manager/configs/known-models.json /opt/agentteams/config/known-models.json
COPY --chmod=755 manager/entrypoint.sh /opt/agentteams/entrypoint.sh

RUN mkdir -p /var/lib/agentteams-manager \
        /opt/agentteams/coding-cli/bin \
    && chmod 700 /var/lib/agentteams-manager \
    && chmod 755 /opt/agentteams/coding-cli \
        /opt/agentteams/coding-cli/bin

WORKDIR /var/lib/agentteams-manager

EXPOSE 18799

HEALTHCHECK --interval=10s --timeout=3s --retries=6 CMD \
  python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:18799/readyz', timeout=2)"

ENTRYPOINT ["/usr/bin/tini", "--", "/opt/agentteams/entrypoint.sh"]
