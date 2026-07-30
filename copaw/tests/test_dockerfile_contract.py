from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_lite_runtime_keeps_the_pinned_copaw_fork() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    lite_start = dockerfile.index("# --- Lite venv: copaw from GitHub fork ---")
    lite_end = dockerfile.index(
        "# ---------------------------------------------------------------------------",
        lite_start,
    )
    lite_install = dockerfile[lite_start:lite_end]

    assert "/tmp/lite-copaw/" in lite_install
    assert '"matrix-nio[e2e]>=0.24.0"' in lite_install
    assert "--no-deps \\\n        /tmp/copaw-worker/" in lite_install


def test_standard_config_overlay_does_not_replace_lite_config() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert (
        'cp /tmp/copaw-worker/src/matrix/config.py '
        '"$STANDARD_SITE/copaw/config/config.py"'
    ) in dockerfile
    assert (
        'cp /tmp/copaw-worker/src/matrix/config.py '
        '"$SITE/copaw/config/config.py"'
    ) not in dockerfile
    assert dockerfile.count(
        "from copaw_worker.hooks import install_tool_hooks; "
        "install_tool_hooks()",
    ) == 2


def test_lite_reme_patch_keeps_required_memory_backends_registered() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    patch = (ROOT / "scripts" / "patch_reme_lazy.py").read_text(
        encoding="utf-8",
    )

    assert 'R.embedding_models.register("openai")(OpenAIEmbeddingModel)' in patch
    assert "memory_backend = \"local\"" in patch
    assert "assert 'openai' in R.embedding_models" in dockerfile
    assert "assert 'local' in R.file_stores" in dockerfile
    assert "assert 'full' in R.file_watchers" in dockerfile
