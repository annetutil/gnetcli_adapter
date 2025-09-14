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

