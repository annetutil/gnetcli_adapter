import traceback

import asyncio
import json
import subprocess
import time

import annet.annlib.command

from annet.deploy import Fetcher, DeployDriver, DeployOptions, DeployResult, apply_deploy_rulebook, ProgressBar
from annet.annlib.command import Command, CommandList
from annet.annlib.netdev.views.hardware import HardwareView
from annet.adapters.netbox.common.models import NetboxDevice
from annet.rulebook import common

from annet.connectors import AdapterWithConfig, AdapterWithName
from typing import Dict, List, Any, Optional, Tuple
from annet.storage import Device

from gnetcli_adapter.progress_tracker import (
    ProgressTracker, ProgressBarTracker, FileProgressTracker, LogProgressTracker, CompositeTracker,
)
from gnetclisdk.client import Credentials, Gnetcli, HostParams, QA, File, GnetcliSessionCmd
from gnetclisdk.exceptions import EOFError
import gnetclisdk.proto.server_pb2 as pb
from pydantic import Field, field_validator, FieldValidationInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import BaseSettings, SettingsConfigDict
import base64
import logging
import threading
import atexit
import shutil
import os.path
try:
    from builtins import (  # type: ignore[attr-defined, unused-ignore]
        ExceptionGroup,
    )
except ImportError:
    from exceptiongroup import (  # type: ignore[no-redef, import-not-found, unused-ignore]
        ExceptionGroup,
    )

breed_to_device = {
    "routeros": "ros",
    "ios12": "cisco",
    "bcom-os": "bcomos",
    "pc": "pc",
    "cuml2": "pc",
    "moxa": "pc",
    "jun10": "juniper",
    "eos4": "arista",
    "h3c": "h3c",
    "vrp85": "huawei",
    "vrp55": "huawei",
}

DEFAULT_GNETCLI_SERVER_CONF = """
logging:
  level: debug
  json: true
port: 0
dev_auth:
  use_agent: true
  ssh_config: true

"""

_local_gnetcli: Optional[threading.Thread] = None
_local_gnetcli_p: Optional[subprocess.Popen] = None
_local_gnetcli_url: Optional[str] = None
LOG_FORMAT = "%(asctime)s - l:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s"
DATE_FMT = "%Y-%m-%d %H:%M:%S"
DEFAULT_GNETCLI_SERVER_PATH = "gnetcli_server"
_logger = logging.getLogger(__name__)


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="gnetcli_", validate_assignment=True)

    url: Optional[str] = None
    server_path: str = DEFAULT_GNETCLI_SERVER_PATH
    server_conf: str = DEFAULT_GNETCLI_SERVER_CONF
    insecure_grpc: bool = Field(default=True)
    login: Optional[str] = None
    password: Optional[str] = None
    dev_login: Optional[str] = None
    dev_password: Optional[str] = None
    ssh_agent_enabled: bool = True

    def make_dev_credentials(self) -> Optional[Credentials]:
        if not self.dev_login and not self.dev_password:
            return None
        return Credentials(self.dev_login, self.dev_password)

    def make_server_credentials(self) -> Optional[str]:
        if self.login and self.password:
            auth_token = base64.b64encode(b"%s:%s" % (self.login.encode(), self.password.encode())).strip().decode()
            return f"Basic {auth_token}"
        return None

    @field_validator("*", mode="before")
    @classmethod
    def not_none(cls, value: Any, info: FieldValidationInfo):
        # NOTE: All fields that are optional for values, will assume the value in
        # "default" (if defined in "Field") if "None" is informed as "value". That
        # is, "None" is never assumed if passed as a "value".
        if (
            cls.model_fields[info.field_name].get_default() is not PydanticUndefined
            and not cls.model_fields[info.field_name].is_required()
            and value is None
        ):
            return cls.model_fields[info.field_name].get_default()
        return value


async def get_config(breed: str) -> List[str]:
    if breed == "routeros":
        return ["/export verbose", "/user export verbose", "/file print terse detail", "/user ssh-keys print terse"]
    elif breed.startswith("ios") or breed.startswith("bcom") or breed.startswith("eltex") or breed.startswith("nxos"):
        return ["show running-config"]
    elif breed.startswith("jun"):
        return ["show configuration"]
    elif breed.startswith("eos4"):
        return ["show running-config | no-more"]
    elif breed.startswith(("h3c", "vrp")):
        return ["display current-configuration"]
    raise Exception("unknown breed %r" % breed)


def check_gnetcli_server(server_path: str, config: str = DEFAULT_GNETCLI_SERVER_CONF):
    global _local_gnetcli
    if not _local_gnetcli:
        t = threading.Thread(target=run_gnetcli_server,
                             kwargs={"server_path": server_path, "config": config})
        t.daemon = True
        t.start()
        time.sleep(1)
        _local_gnetcli = t
    if _local_gnetcli_p is None:
        raise Exception("server failed")


def cleanup():
    if _local_gnetcli_p is not None:
        _local_gnetcli_p.kill()


atexit.register(cleanup)


async def gather_with_concurrency(n: int, *coros: list[asyncio.Task]):
    if n == 0:
        return await asyncio.gather(*coros)
    semaphore = asyncio.Semaphore(n)

    async def sem_coro(coro):
        async with semaphore:
            return await coro

    return await asyncio.gather(*(sem_coro(c) for c in coros))


def get_device_ip(dev: Device) -> Optional[str]:
    if isinstance(dev, NetboxDevice):
        if dev.primary_ip:
            return dev.primary_ip.address.split("/")[0]
        for iface in dev.interfaces:
            for ip in iface.ip_addresses:
                return ip.address.split("/")[0]
    return None


def run_gnetcli_server(server_path: str, config: str = DEFAULT_GNETCLI_SERVER_CONF):
    global _local_gnetcli_p
    global _local_gnetcli_url
    abs_path: Optional[str] = shutil.which(server_path)
    if not abs_path and server_path == DEFAULT_GNETCLI_SERVER_PATH:
        abs_path = shutil.which(os.path.expanduser("~/go/bin/gnetcli_server"))
    gnetcli_abs_path: str = abs_path or server_path
    _logger.info("starting gnetcli server %s", gnetcli_abs_path)
    try:
        proc = subprocess.Popen(
            [gnetcli_abs_path, "--conf-file", "-"],
            stdout=subprocess.DEVNULL, # we do not read stdout
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            bufsize=1,
            universal_newlines=True,
        )
    except Exception as e:
        logging.exception("server exec error %s", e)
        raise
    proc.stdin.write(config)
    proc.stdin.close()
    _local_gnetcli_p = proc
    while True:
        output = proc.stderr.readline()
        if output == "" and proc.poll() is not None:
            break
        if output:
            _logger.debug("gnetcli output: %s", output.strip())
            if _local_gnetcli_url is None:
                try:
                    data = json.loads(output)
                except Exception:
                    pass
                else:
                    if data.get("msg") == "init tcp socket":
                        _local_gnetcli_url = data.get("address")
                    if data.get("msg") == "init unix socket":
                        _local_gnetcli_url = "unix:" + data.get("path")
                    if data.get("level") == "panic":
                        _logger.error("gnetcli error %s", data)
    _logger.debug("set gnetcli server exit code %s", proc.returncode)
    rc = proc.poll()
    return rc


class GnetcliFetcher(Fetcher, AdapterWithConfig, AdapterWithName):
    def __init__(
        self,
        url: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        dev_login: Optional[str] = None,
        dev_password: Optional[str] = None,
        ssh_agent_enabled: bool = True,
        server_path: Optional[str] = None,
        server_conf: str = DEFAULT_GNETCLI_SERVER_CONF,
    ):
        conf_args = {
            "login": login,
            "password": password,
            "dev_login": dev_login,
            "dev_password": dev_password,
            "server_path": server_path,
            "url": url,
            "server_conf": server_conf,
            "ssh_agent_enabled": ssh_agent_enabled,
        }
        self.conf = AppSettings(**{k: v for k,v in conf_args.items() if v is not None})
        self.api = make_api(self.conf)

    @classmethod
    def name(cls) -> str:
        return "gnetcli"

    @classmethod
    def with_config(cls, **kwargs: Any) -> Fetcher:
        return cls(**kwargs)

    async def fetch_packages(self, devices: List[Device], processes: int = 1, max_slots: int = 0):
        if not devices:
            return {}, {}
        # TODO: implement fetch_packages
        return {}, {}

    async def fetch(self,
                    devices: List[Device],
                    files_to_download: Optional[Dict[Device, List[str]]] = None,
                    processes: int = 1,
                    max_slots: int = 0,
    ):
        running = {}
        failed_running = {}

        tasks = {}
        for device in devices:
            if files_to_download or device.is_pc():
                if files_to_download:
                    files = files_to_download.get(device, [])
                    task = asyncio.create_task(self.adownload_dev(device=device, files=files))
                    tasks[task] = device
                else:
                    running[device] = {}
            else:
                task = asyncio.create_task(self.afetch_dev(device=device))
                tasks[task] = device

        if not tasks:
            return running, failed_running

        done, pending = await asyncio.wait(list(tasks), return_when=asyncio.ALL_COMPLETED)
        for task in done:
            device = tasks[task]
            if (exc := task.exception()) is not None:
                failed_running[device] = exc
            else:
                running[device] = task.result()
        return running, failed_running

    async def afetch_dev(self, device: Device) -> str:
        cmds = await get_config(breed=device.breed)
        # map annet breed to gnetcli device type
        gnetcli_device = breed_to_device.get(device.breed, device.breed)
        dev_result = []
        ip = get_device_ip(device)
        for cmd in cmds:
            res = await self.api.cmd(
                hostname=device.fqdn,
                cmd=cmd,
                host_params=HostParams(
                    credentials=self.conf.make_dev_credentials(),
                    device=gnetcli_device,
                    ip=ip,
                ),
            )
            if res.status != 0:
                raise Exception("cmd error %s" % res)
            dev_result.append(res.out)
        return b"\n".join(dev_result).decode()

    async def adownload_dev(self, device: Device, files: List[str]) -> Dict[str, str]:
        gnetcli_device = breed_to_device.get(device.breed, device.breed)
        ip = get_device_ip(device)
        downloaded = await self.api.download(
            hostname=device.fqdn,
            paths=files,
            host_params=HostParams(
                credentials=self.conf.make_dev_credentials(),
                device=gnetcli_device,
                ip=ip,
            ),
        )
        res: Dict[str, str] = {}
        for file, file_data in downloaded.items():
            res[file] = file_data.content.decode()
        return res


def parse_annet_qa(qa: list[annet.annlib.command.Question]) -> list[QA]:
    res: list[QA] = []
    for annet_qa in qa:
        q = annet_qa.question
        if annet_qa.is_regexp:
            q = f"/{annet_qa.question}/"
        res.append(QA(question=q, answer=annet_qa.answer, not_send_nl=annet_qa.not_send_nl))
    return res


def make_api(conf: AppSettings) -> Gnetcli:
    gnetcli_url = _local_gnetcli_url
    if not conf.url:
        check_gnetcli_server(server_path=conf.server_path, config=conf.server_conf)
        if _local_gnetcli_url is None:
            _logger.info("waiting for _local_gnetcli_url appears")
            start = time.monotonic()
            while time.monotonic() - start < 5:
                if _local_gnetcli_p is not None and _local_gnetcli_p.returncode is not None:
                    raise Exception("gnetcli server died with code %s" % _local_gnetcli_p.returncode)
        gnetcli_url = _local_gnetcli_url
    else:
        gnetcli_url = conf.url
    auth_token = conf.make_server_credentials()
    api = Gnetcli(
        server=gnetcli_url,
        auth_token=auth_token,
        insecure_grpc=conf.insecure_grpc,
        user_agent="annet",
    )
    return api


class GnetcliDeployer(DeployDriver, AdapterWithConfig, AdapterWithName):
    def __init__(
        self,
        url: Optional[str] = None,
        login: Optional[str] = None,
        password: Optional[str] = None,
        dev_login: Optional[str] = None,
        dev_password: Optional[str] = None,
        ssh_agent_enabled: bool = True,
        server_path: Optional[str] = None,
        server_conf: Optional[str] = DEFAULT_GNETCLI_SERVER_CONF,
        logs_dir: Optional[str] = None,
    ):
        conf_args = {
            "login": login,
            "password": password,
            "dev_login": dev_login,
            "dev_password": dev_password,
            "server_path": server_path,
            "url": url,
            "server_conf": server_conf,
            "ssh_agent_enabled": ssh_agent_enabled,
        }
        self.conf = AppSettings(**{k: v for k,v in conf_args.items() if v is not None})
        self.api = make_api(self.conf)
        self.logs_dir = logs_dir

    @classmethod
    def name(cls) -> str:
        return "gnetcli"

    @classmethod
    def with_config(cls, **kwargs: Any) -> DeployDriver:
        return cls(**kwargs)

    async def bulk_deploy(self, deploy_cmds: dict[Device, CommandList], args: DeployOptions, progress_bar: ProgressBar | None = None) -> DeployResult:
        if progress_bar:
            for host, cmds in deploy_cmds.items():
                progress_bar.set_progress(host.fqdn, 0, len(cmds))
        max_parallel = 1000
        try:
            # temporary, for supporting of old annet
            arg_max_parallel = args.max_parallel
            if arg_max_parallel > 0:
                max_parallel = arg_max_parallel
        except Exception:
            pass
        deploy_items = deploy_cmds.items()
        if max_parallel == 1:
            result = await self.serial_deploy(deploy_items, args, progress_bar)
        else:
            result = await gather_with_concurrency(
                max_parallel,
                *[self.deploy(device, cmds, args, progress_bar) for device, cmds in deploy_items],
            )
        res = DeployResult(hostnames=[], results={}, durations={}, original_states={})
        res.add_results(results={dev.fqdn: dev_res for (dev, _), dev_res in zip(deploy_items, result)})
        return res

    def _get_files(self, cmds: dict[str, Any]) -> dict[str, File]:
        return {
            file: File(content=content, status=None)
            for file, content in cmds["files"].items()
        }

    def _get_reload_cmds(self, cmds: dict[str, Any]):
        reload_cmds: dict[str, CommandList] = {}
        for file, cmd in cmds["cmds"].items():
            if isinstance(cmd, bytes):
                cmd = cmd.decode()
            reload_cmds[file] = CommandList([Command(cmd, suppress_nonzero=True) for cmd in cmd.splitlines()])
        return reload_cmds

    def _get_total(
            self, command_groups: list[tuple[str, CommandList]], files: dict[str, File],
    ) -> int:
        run_cmds = 0
        for _, cmds in command_groups:
            run_cmds += len(cmds)
        if files:
            run_cmds += 1
        return run_cmds

    def _init_progress_tracker(self, device: Device, progress_bar: ProgressBar | None) -> ProgressTracker:
        tracker = CompositeTracker(LogProgressTracker(device))
        if progress_bar:
            tracker.add_tracker(ProgressBarTracker(device, progress_bar))
        if self.logs_dir:
            tracker.add_tracker(FileProgressTracker(device, self.logs_dir))
        return tracker

    async def serial_deploy(self, deploy_items, args, progress_bar: ProgressBar | None = None):
        res = {}
        for device, cmds in deploy_items:
            if progress_bar:
                try:
                    # hack, no way to do this using interface ProgressBar
                    for no, fqdn in enumerate(progress_bar.tiles_params):
                        if fqdn == device.fqdn:
                            progress_bar._set_active_tile(no)
                            break
                except Exception:
                    pass
            dev_res = await self.deploy(device, cmds, args, progress_bar)
            res[device] = dev_res
        return res

    async def deploy(
            self,
            device: Device,
            cmds: CommandList,
            args: DeployOptions,
            progress_bar: ProgressBar | None = None,
    ) -> Exception | None:
        gnetcli_device = breed_to_device.get(device.breed, device.breed)
        ip = get_device_ip(device)
        host_params = HostParams(
            credentials=self.conf.make_dev_credentials(),
            device=gnetcli_device,
            ip=ip,
        )
        command_groups: list[tuple[str, CommandList]]= []
        if isinstance(cmds, dict): # PC
            files = self._get_files(cmds)
            if args.entire_reload.value == "yes":
                for file, cmdlist in self._get_reload_cmds(cmds).items():
                    command_groups.append((f"Reload {file}", cmdlist))
        else:
            files = {}
            # treat each command as a group
            for cmd in cmds:
                command_groups.append(("Run command", CommandList([cmd])))

        with self._init_progress_tracker(device, progress_bar) as tracker:
            tracker.set_total(self._get_total(command_groups, files))
            try:
                seen_exc, results = await self._deploy(
                    device=device,
                    host_params=host_params,
                    command_groups=command_groups,
                    tracker=tracker,
                    files=files,
                )
            except Exception as e:
                trace = traceback.format_exc()
                tracker.command_done_error(trace)
                seen_exc = [e]
                results = []
            if seen_exc:
                tracker.finish_err(f"Seen {len(seen_exc)} exceptions")
            else:
                tracker.finish_ok("All done")

            if seen_exc:
                return ExceptionGroup("Deploy failed with exceptions", seen_exc)
            return None

    async def _deploy(
            self,
            device: Device,
            host_params: HostParams,
            command_groups: list[tuple[str, CommandList]],
            files: dict[str, File],
            tracker: ProgressTracker,
    ) -> tuple[list[Exception], list[pb.CMDResult]]:
        seen_exc = []
        results = []
        if files:
            tracker.upload_files(list(files))
            await self.api.upload(hostname=device.fqdn, files=files, host_params=host_params)
            tracker.files_uploaded()

        old_group_name = ""
        async with self.api.cmd_session(hostname=device.fqdn) as session:
            for group_number, (group_name, cmdlist) in enumerate(command_groups):
                if group_name != old_group_name:
                    tracker.start_group(group_name)
                    old_group_name = group_name
                for cmd_number, cmd in enumerate(cmdlist):
                    tracker.run_command(cmd.cmd)
                    try:
                        res = await session.cmd(
                            cmd=cmd.cmd,
                            cmd_timeout=cmd.timeout,
                            host_params=host_params,
                            qa=parse_annet_qa(cmd.questions or []),
                            trace=True,
                        )
                    except EOFError as e:
                        # we can't execute subsequent commands
                        if cmd.suppress_eof:
                            # some commands left
                            if group_number + 1  != len(command_groups) or cmd_number + 1 != len(cmdlist):
                                tracker.command_done_error("EOF detected before all commands executed.")
                                seen_exc.append(Exception("EOF detected before all commands executed."))
                            tracker.command_done_error_suppressed("Suppressed EOF")
                            return seen_exc, results
                        seen_exc.append(e)
                        tracker.command_done_error("Unexpected EOFError")
                        return seen_exc, results
                    if res.status == 0:
                        tracker.command_done_ok(res)
                        results.append(res)
                    else:
                        e = Exception("cmd %s error %s status %s", cmd, res.error, res.status)
                        seen_exc.append(e)
                        if cmd.suppress_nonzero:
                            tracker.command_done_ok(res)
                            break  # go to next command group
                        else:
                            tracker.command_done_error(res.error.decode())
                            return seen_exc, results
        return seen_exc, results

    def apply_deploy_rulebook(
        self,
        hw: HardwareView,
        cmd_paths: Dict[Tuple[str, ...], Dict[str, Any]],
        do_finalize: bool = True,
        do_commit: bool = True,
    ):
        res = apply_deploy_rulebook(hw=hw, cmd_paths=cmd_paths, do_finalize=do_finalize, do_commit=do_commit)
        return res

    def build_configuration_cmdlist(
        self, hw: HardwareView, do_finalize: bool = True, do_commit: bool = True, path: str = None,
    ):
        res = common.apply(hw, do_commit=do_commit, do_finalize=do_finalize, path=path)
        return res

    def build_exit_cmdlist(self, hw: HardwareView) -> CommandList:
        ret = CommandList()
        if hw.Huawei or hw.H3C:
            ret.add_cmd(Command("quit", suppress_eof=True))
        return ret
