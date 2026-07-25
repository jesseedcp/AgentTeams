from importlib.metadata import version


def test_qwenpaw_runtime_uses_the_compatible_acp_schema() -> None:
    from acp import SetSessionModelResponse

    assert version("agent-client-protocol") == "0.10.1"
    assert SetSessionModelResponse is not None
