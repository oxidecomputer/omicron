# `gateway-sp-comms`

Communicate with SPs across the management network.

## DTrace Probes

This crate currently provides three DTrace probes: receiving raw packets,
receiving responses to requests, receiving serial console messages.

Raw packets:

```
% sudo dtrace -n 'gateway_sp_comms*:::recv_packet { printf("%s, %s", copyinstr(arg0), copyinstr(arg1)); tracemem(copyin(arg2, arg3), 128, arg3); }'
dtrace: description 'gateway_sp_comms*:::recv_packet ' matched 1 probe
CPU     ID                    FUNCTION:NAME
  6   2961 _ZN16gateway_sp_comms17management_switch9recv_task28_$u7b$$u7b$closure$u7d$$u7d$17h2bdb4a0aa5dcced5E:recv_packet {"ok":"127.0.0.1:23457"}, {"ok":1}
             0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f  0123456789abcdef
         0: 01 00 00 00 01 73 70 33 00 00 00 00 00 00 00 00  .....sp3........
        10: 00 00 00 00 00 24 00 00 00 00 00 00 00 06 00 68  .....$.........h
        20: 65 6c 6c 6f 0a                                   ello.
```

Responses:

```
% sudo dtrace -n 'gateway_sp_comms*:::recv_response { printf("%s, %u, %s", copyinstr(arg0), arg1, copyinstr(arg2)); }'
dtrace: description 'gateway_sp_comms*:::recv_response ' matched 1 probe
CPU     ID                    FUNCTION:NAME
  5   2962 _ZN16gateway_sp_comms12recv_handler11RecvHandler15handle_response17h28a3ce4546c4e1bdE:recv_response {"ok":0}, 0, {"ok":{"Ok":{"IgnitionState":{"id":17,"flags":{"bits":3}}}}}
  2   2962 _ZN16gateway_sp_comms12recv_handler11RecvHandler15handle_response17h28a3ce4546c4e1bdE:recv_response {"ok":1}, 1, {"ok":{"Ok":{"SpState":{"serial_number":[0,0,0,0,1,1,1,1,2,2,2,2,3,3,3,3]}}}}
```

Serial console messages:

```
% sudo dtrace -n 'gateway_sp_comms*:::recv_serial_console { printf("%u, %s, %u", arg0, copyinstr(arg0), arg2); tracemem(copyin(arg3, arg4), 128, arg4); }'
dtrace: description 'gateway_sp_comms*:::recv_serial_console ' matched 1 probe
CPU     ID                    FUNCTION:NAME
  2   2963 _ZN16gateway_sp_comms12recv_handler11RecvHandler21handle_serial_console17hba2bd6ac4422e295E:recv_serial_console 105553180037200, {"ok":1}, 42
             0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f  0123456789abcdef
         0: 68 65 6c 6c 6f 21 0a                             hello!.
```

TODO EXPAND
