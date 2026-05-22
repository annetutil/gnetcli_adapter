# gnetcli_adapter
This package provides deployer and fetcher adapters for Annet

# Examples

## Using specified login and password

cat ~/.annet/context.yml
```yaml
fetcher:
  default:
    adapter: gnetcli
    params: &gnetcli
      dev_login: mylogin
      dev_password: mypassword
deployer:
  default:
    adapter: gnetcli
    params:
      <<: *gnetcli
...
context:
  default:
    fetcher: default
    deployer: default
selected_context: default
```

## Using tunnel through master SSH-connection

https://en.wikibooks.org/wiki/OpenSSH/Cookbook/Multiplexing

cat ~/.ssh/context.yml
```
Host myhost*
    ProxyJump mybastion

Host mybastion
    ControlMaster auto
    ControlPath ~/.ssh/mastersockets/%r@%h:%p
    ControlPersist 120m
```

`~/.annet/context.yml` the same because gnetcli read .ssh/config by default.

## Connecting to an externally-running gnetcli_server

If `gnetcli_server` is already running on a bastion host, set `url` and the
adapter will connect to it instead of spawning a local server binary. `login`
and `password` authenticate to the gnetcli server itself (Basic auth);
`dev_login`/`dev_password` are still the network device credentials.

```yaml
fetcher:
  default:
    adapter: gnetcli
    params: &gnetcli
      url: 192.0.2.10:50051
      login: gnetcli-user
      password: gnetcli-secret
      dev_login: mylogin
      dev_password: mypassword
deployer:
  default:
    adapter: gnetcli
    params:
      <<: *gnetcli
```

When `url` is unset, the adapter starts a local `gnetcli_server` subprocess
(`server_path` defaults to `gnetcli_server` on `$PATH`).
