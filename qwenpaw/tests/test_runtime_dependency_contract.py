from importlib.metadata import version

import pytest


def test_qwenpaw_runtime_uses_the_compatible_acp_schema() -> None:
    pytest.importorskip("acp")
    from acp import SetSessionModelResponse

    assert version("agent-client-protocol") == "0.10.1"
    assert SetSessionModelResponse is not None
