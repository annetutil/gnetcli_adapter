from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import gnetclisdk.proto.server_pb2 as pb
import pytest
from annet.annlib.command import Command, CommandList
from gnetclisdk.client import File, HostParams

from gnetcli_adapter.gnetcli_adapter import GnetcliDeployer, GnetcliFetcher


def test_make_api_uses_url_without_starter():
    fetcher = GnetcliFetcher(url="127.0.0.1:50050")

    async def run():
        with patch("gnetcli_adapter.gnetcli_adapter.GnetcliStarter") as starter:
            async with fetcher.make_api() as api:
                assert api._server == "127.0.0.1:50050"
            starter.assert_not_called()

    asyncio.run(run())


def test_routeros7_uses_embedded_ros_device():
    fetcher = GnetcliFetcher(url="127.0.0.1:50050")
    device = SimpleNamespace(
        breed="routeros7",
        fqdn="router.example.net",
        primary_ip=None,
        interfaces=[],
    )
    api = Mock()
    api.cmd = AsyncMock(return_value=pb.CMDResult(status=0, out=b"config"))

    result = asyncio.run(fetcher.afetch_dev(api=api, device=device))

    assert result == "config\nconfig\nconfig\nconfig"
    assert api.cmd.await_count == 4
    assert all(call.kwargs["host_params"].device == "ros" for call in api.cmd.await_args_list)


@pytest.mark.parametrize(
    ("streamer_type", "dev_port", "expected_streamer_type"),
    [
        (None, None, pb.StreamerType_ssh),
        ("ssh", 10022, pb.StreamerType_ssh),
        ("telnet", 23, pb.StreamerType_telnet),
    ],
)
def test_fetch_propagates_port_and_streamer_type(streamer_type, dev_port, expected_streamer_type):
    fetcher = GnetcliFetcher(
        url="127.0.0.1:50050",
        dev_port=dev_port,
        streamer_type=streamer_type,
    )
    device = SimpleNamespace(breed="jun10", fqdn="router.example.net", primary_ip=None, interfaces=[])
    api = Mock()
    api.cmd = AsyncMock(return_value=pb.CMDResult(status=0, out=b"config"))

    result = asyncio.run(fetcher.afetch_dev(api=api, device=device))

    assert result == "config"
    host_params = api.cmd.await_args.kwargs["host_params"]
    assert host_params.port == dev_port
    assert host_params.streamer_type == expected_streamer_type


def test_fetch_applies_minimum_timeouts():
    fetcher = GnetcliFetcher(
        url="127.0.0.1:50050",
        min_cmd_timeout=120,
        min_read_timeout=60,
    )
    device = SimpleNamespace(breed="jun10", fqdn="router.example.net", primary_ip=None, interfaces=[])
    api = Mock()
    api.cmd = AsyncMock(return_value=pb.CMDResult(status=0, out=b"config"))

    result = asyncio.run(fetcher.afetch_dev(api=api, device=device))

    assert result == "config"
    assert api.cmd.await_args.kwargs["cmd_timeout"] == 120
    assert api.cmd.await_args.kwargs["read_timeout"] == 60


def test_deploy_propagates_port_and_streamer_type():
    deployer = GnetcliDeployer(
        url="127.0.0.1:50050",
        dev_port=23,
        streamer_type="telnet",
    )
    deployer._deploy = AsyncMock(return_value=([], []))
    device = SimpleNamespace(breed="jun10", fqdn="router.example.net", primary_ip=None, interfaces=[])

    result = asyncio.run(
        deployer.deploy(
            api=Mock(),
            device=device,
            cmds=CommandList(),
            args=SimpleNamespace(),
        )
    )

    assert result is None
    host_params = deployer._deploy.await_args.kwargs["host_params"]
    assert host_params.port == 23
    assert host_params.streamer_type == pb.StreamerType_telnet


def test_deploy_applies_minimum_timeouts_without_lowering_larger_values():
    deployer = GnetcliDeployer(
        url="127.0.0.1:50050",
        min_cmd_timeout=120,
        min_read_timeout=60,
    )
    session = Mock()
    session.cmd = AsyncMock(
        side_effect=[
            pb.CMDResult(status=0, out=b"first"),
            pb.CMDResult(status=0, out=b"second"),
        ]
    )

    @asynccontextmanager
    async def cmd_session(**_kwargs):
        yield session

    api = Mock()
    api.cmd_session = cmd_session
    commands = CommandList(
        [
            Command("short timeout", timeout=30, read_timeout=10),
            Command("long timeout", timeout=180, read_timeout=90),
        ]
    )

    seen_exc, _ = asyncio.run(
        deployer._deploy(
            api=api,
            device=Mock(fqdn="device.example.net"),
            host_params=HostParams(device="cisco"),
            command_groups=[("Run command", commands)],
            files={},
            tracker=Mock(),
        )
    )

    assert seen_exc == []
    assert [(call.kwargs["cmd_timeout"], call.kwargs["read_timeout"]) for call in session.cmd.await_args_list] == [
        (120, 60),
        (180, 90),
    ]


@pytest.mark.parametrize(
    "params",
    [
        {"dev_port": 0},
        {"dev_port": 65536},
        {"streamer_type": "console"},
        {"min_cmd_timeout": 0},
        {"min_read_timeout": -1},
    ],
)
def test_rejects_invalid_connection_params(params):
    with pytest.raises(ValueError):
        GnetcliFetcher(url="127.0.0.1:50050", **params)


def test_download_preserves_ok_and_not_found_file_statuses():
    fetcher = GnetcliFetcher(url="127.0.0.1:50050")
    device = SimpleNamespace(breed="pc", fqdn="host.example.net", primary_ip=None, interfaces=[])
    api = Mock()
    api.download = AsyncMock(
        return_value={
            "/etc/config": File(content=b"config", status=pb.FileStatus_ok),
            "/etc/missing": File(content=b"", status=pb.FileStatus_not_found),
        }
    )

    result = asyncio.run(
        fetcher.adownload_dev(
            api=api,
            device=device,
            files=["/etc/config", "/etc/missing"],
        )
    )

    assert result == {"/etc/config": "config", "/etc/missing": None}


@pytest.mark.parametrize(
    "status",
    [pb.FileStatus_notset, pb.FileStatus_error, pb.FileStatus_is_dir],
)
def test_download_rejects_unsuccessful_file_statuses(status):
    fetcher = GnetcliFetcher(url="127.0.0.1:50050")
    device = SimpleNamespace(breed="pc", fqdn="host.example.net", primary_ip=None, interfaces=[])
    api = Mock()
    api.download = AsyncMock(return_value={"/etc/config": File(content=b"", status=status)})

    with pytest.raises(RuntimeError, match=pb.FileStatus.Name(status)):
        asyncio.run(fetcher.adownload_dev(api=api, device=device, files=["/etc/config"]))


def test_serial_deploy_preserves_credentials_and_results():
    deployer = GnetcliDeployer(
        url="127.0.0.1:50050",
        dev_login="device-login",
        dev_password="device-password",
    )
    api = Mock()
    args = SimpleNamespace(max_parallel=1)
    device_ok = Mock(fqdn="ok.example.net")
    device_failed = Mock(fqdn="failed.example.net")
    cmds_ok = CommandList()
    cmds_failed = CommandList()
    deploy_error = RuntimeError("deploy failed")

    async def deploy_result(_api, device, _cmds, _args, _progress_bar, _credentials):
        if device is device_failed:
            return deploy_error
        return None

    deployer.deploy = AsyncMock(side_effect=deploy_result)

    result = asyncio.run(
        deployer._bulk_deploy(
            api=api,
            deploy_cmds={device_ok: cmds_ok, device_failed: cmds_failed},
            args=args,
        )
    )

    assert result.hostnames == ["ok.example.net", "failed.example.net"]
    assert result.results == {
        "ok.example.net": None,
        "failed.example.net": deploy_error,
    }
    assert deployer.deploy.await_count == 2
    for call, device, cmds in zip(
        deployer.deploy.await_args_list,
        (device_ok, device_failed),
        (cmds_ok, cmds_failed),
    ):
        assert call.args[:5] == (api, device, cmds, args, None)
        credentials = call.args[5]
        assert credentials.login == "device-login"
        assert credentials.password == "device-password"


def test_suppress_errors_does_not_fail_deploy_and_continues_commands():
    deployer = GnetcliDeployer(url="127.0.0.1:50050")
    suppressed_result = pb.CMDResult(status=1, error=b"ignored error")
    successful_result = pb.CMDResult(status=0, out=b"done")
    session = Mock()
    session.cmd = AsyncMock(side_effect=[suppressed_result, successful_result])

    @asynccontextmanager
    async def cmd_session(**_kwargs):
        yield session

    api = Mock()
    api.cmd_session = cmd_session
    tracker = Mock()
    commands = CommandList(
        [
            Command("ignored command", suppress_errors=True),
            Command("next command"),
        ]
    )

    seen_exc, results = asyncio.run(
        deployer._deploy(
            api=api,
            device=Mock(fqdn="device.example.net"),
            host_params=HostParams(device="cisco"),
            command_groups=[("Run command", commands)],
            files={},
            tracker=tracker,
        )
    )

    assert seen_exc == []
    assert results == [successful_result]
    assert session.cmd.await_count == 2
    tracker.command_done_error_suppressed.assert_called_once_with("ignored error")
    tracker.command_done_error.assert_not_called()


def test_suppress_nonzero_does_not_fail_deploy_and_continues_commands():
    deployer = GnetcliDeployer(url="127.0.0.1:50050")
    suppressed_result = pb.CMDResult(status=1, error=b"unsupported command")
    same_group_result = pb.CMDResult(status=0, out=b"same group done")
    next_group_result = pb.CMDResult(status=0, out=b"next group done")
    session = Mock()
    session.cmd = AsyncMock(side_effect=[suppressed_result, same_group_result, next_group_result])

    @asynccontextmanager
    async def cmd_session(**_kwargs):
        yield session

    api = Mock()
    api.cmd_session = cmd_session
    tracker = Mock()
    command_groups = [
        (
            "First group",
            CommandList(
                [
                    Command("unsupported command", suppress_nonzero=True),
                    Command("same group command"),
                ]
            ),
        ),
        ("Second group", CommandList([Command("next group command")])),
    ]

    seen_exc, results = asyncio.run(
        deployer._deploy(
            api=api,
            device=Mock(fqdn="device.example.net"),
            host_params=HostParams(device="pc"),
            command_groups=command_groups,
            files={},
            tracker=tracker,
        )
    )

    assert seen_exc == []
    assert results == [same_group_result, next_group_result]
    assert [call.kwargs["cmd"] for call in session.cmd.await_args_list] == [
        "unsupported command",
        "same group command",
        "next group command",
    ]
    tracker.command_done_error_suppressed.assert_called_once_with("unsupported command")
    tracker.command_done_error.assert_not_called()
