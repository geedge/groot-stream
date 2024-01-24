# IPFIX
> IPFIX source collector
## Description
IPFIX source collector is used to collect IPFIX data from network devices. It supports UDP protocol.
## Source Options
In order to use the IPFIX connector, the following dependencies are required. They can be download by Nexus Maven Repository.

| Datasource | Supported Versions | Maven                                                                                                                                    |
|------------|--------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| IPFIX      | Universal          | [Download](http://192.168.40.153:8099/service/local/repositories/platform-release/content/com/geedgenetworks/connector-ipfix-collector/) |

IPFIX source custom properties.

| Name                                    | Type    | Required | Default   | Description                                                                                                                                                   |
|-----------------------------------------|---------|----------|-----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| .port.range                             | Integer | Yes      | -         | UDP port range                                                                                                                                                |
| buffer.size                             | Integer | No       | 65535     | The maximum size of packet for UDP                                                                                                                            |
| receive.buffer.size                     | Integer | No       | 104857600 | UDP receive buffer size in bytes                                                                                                                              |
| service.discovery.registry.mode         | String  | No       | -         | Service discovery registry mode, support `0(nacos)` and `1(consul)`                                                                                           |
| service.discovery.service.name          | String  | No       | -         | Service discovery service name                                                                                                                                |
| service.discovery.health.check.interval | Integer | No       | -         | Service discovery health check interval in milliseconds                                                                                                       |
| service.discovery.nacos.server.addr     | String  | No       | -         | A list of host/port pairs to use for establishing the initial connection to the Nacos cluster. This list should be in the form `host1:port1,host2:port2,...`. |
| service.discovery.nacos.username        | String  | No       | -         | The username to use to connect to Nacos.Need specify `ipfix.service.discovery.registry.mode` to `0(nacos)`                                                    |
| service.discovery.nacos.password        | String  | No       | -         | The password to use to connect to Nacos.Need specify `ipfix.service.discovery.registry.mode` to `0(nacos)`                                                    |
| service.discovery.nacos.namespace       | String  | No       | -         | The namespace to use to connect to Nacos.Need specify `ipfix.service.discovery.registry.mode` to `0(nacos)`                                                   |
| service.discovery.nacos.group           | String  | No       | -         | The group to use to connect to Nacos.Need specify `ipfix.service.discovery.registry.mode` to `0(nacos)`                                                       |
| service.discovery.consul.server.addr    | String  | No       | -         | Consul server address. Need specify `ipfix.service.discovery.registry.mode` to `1(consul)`                                                                    |
| service.discovery.consul.server.port    | Integer | No       | -         | Consul server port. Need specify `ipfix.service.discovery.registry.mode` to `1(consul)`                                                                       |
| service.discovery.consul.token          | String  | No       | -         | Consul token. Need specify `ipfix.service.discovery.registry.mode` to `1(consul)`                                                                             |

## Example