import pytest

from agentteams_manager.main import run_application


class FakeApplication:
    def __init__(self) -> None:
        self.ran = False

    async def run(self) -> None:
        self.ran = True


@pytest.mark.asyncio
async def test_run_application_enters_daemon_lifecycle() -> None:
    application = FakeApplication()

    await run_application(application, install_signals=False)

    assert application.ran
