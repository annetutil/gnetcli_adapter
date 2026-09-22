import asyncio
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, Mock, patch

import pytest
import gnetclisdk.proto.server_pb2 as pb
from annet.annlib.command import Command, CommandList
from gnetclisdk.client import HostParams
from gnetcli_adapter.gnetcli_adapter import GnetcliDeployer


@pytest.mark.parametrize('delay,status', [(0.25, 0), (0, 0), (0.25, 1)])
def test_delay_between_commands(delay, status):
    events = []

    async def run_command(**kwargs):
        events.append(kwargs['cmd'])
        return pb.CMDResult(status=status)

    async def sleep(seconds):
        events.append(('sleep', seconds))

    session = Mock()
    session.cmd = AsyncMock(side_effect=run_command)

    @asynccontextmanager
    async def cmd_session(**kwargs):
        yield session

    api = Mock()
    api.cmd_session = cmd_session
    commands = CommandList([
        Command('undo peer 2001:db8::1', delay_after=delay),
        Command('peer 2001:db8::1 group SPINES'),
    ])
    with patch('gnetcli_adapter.gnetcli_adapter.asyncio.sleep', side_effect=sleep):
        asyncio.run(GnetcliDeployer(url='127.0.0.1:50050')._deploy(
            api=api, device=Mock(fqdn='device.example.net'),
            host_params=HostParams(device='h3c'),
            command_groups=[('Run command', commands)], files={}, tracker=Mock(),
        ))
    expected = ['undo peer 2001:db8::1']
    if status == 0:
        if delay:
            expected.append(('sleep', delay))
        expected.append('peer 2001:db8::1 group SPINES')
    assert events == expected
