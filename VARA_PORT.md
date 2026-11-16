# VARA Control Port (linbpq64)

modem73 can expose a **VARA-compatible command/data TCP interface** so [linbpq64](https://github.com/g8bpq/linbpq) can drive it with `DRIVER=VARA`, the same way it drives [xara](https://github.com/kg9ni/xara).

This is a **host-API** compatibility layer. Over the air, sessions use modem73 framing (`V73…`), not the VARA HF/FM physical layer. Two modem73 stations with the VARA port enabled can connect to each other; they will not interoperate with real VARA or xara on RF.

## Ports

| Port | Role | Default |
|------|------|---------|
| Command (`--vara-port`) | ASCII commands, `\r`-terminated replies | off (`0`) |
| Data (`vara-port + 1`) | Raw session bytes | — |

Defaults match xara/linbpq practice when enabled: **8300** command, **8301** data, bind **127.0.0.1**.

## Enable

```bash
modem73 --headless --callsign N0CALL --vara-port 8300
```

Optional:

```bash
modem73 --vara-port 8300 --vara-bind 127.0.0.1 --vara-retries 15
```

Config file keys (also saved from the TUI settings file):

```
vara_port=8300
vara_bind_address=127.0.0.1
vara_retries=15
```

The existing JSON control port (`8073`) and KISS port (`8001`) are unchanged.

## linbpq64

```
PORT
 PORTNUM=4
 ID=MODEM73 VARA
 DRIVER=VARA
 CONFIG
  ADDR 127.0.0.1 8300
  RETRIES 5
ENDPORT
```

- `ADDR host cmdport` — linbpq opens `cmdport` and `cmdport+1` for data.
- Put `RETRIES N` in the VARA `CONFIG` block (or rely on modem73’s `--vara-retries` / `vara_retries` default). linbpq forwards that line to the modem; the modem uses it for **outbound CONNECT attempts**.

## RETRIES

Matches xara / VARA host semantics:

| Host command | Effect |
|--------------|--------|
| `RETRIES n` (1–255) | Set connect-attempt budget; reply `OK` |
| `RETRIES 0` | Restore the previous value; reply `OK` |
| Invalid / out of range | Leave current value; reply `OK` |

When linbpq (or another host) issues `CONNECT mycall remote`, modem73 transmits a connect request up to **N** times (`N` = current RETRIES, default **15** like xara HF). If no peer answers after N attempts, it reports `DISCONNECTED`.

Local default if the host never sends `RETRIES`: `--vara-retries` / `vara_retries` (default 15).

## Command highlights

Framing: one command per line ending in `\r` (`\n` ignored). Replies end with `\r`.

| Command | Notes |
|---------|--------|
| `VERSION` | `VERSION VARA HF v4.9.0` (xara-compatible string for hosts) |
| `MYCALL …` | Up to 5 calls; later `REGISTERED` + `ENCRYPTION DISABLED` |
| `LISTEN ON` / `OFF` | Arm/disarm inbound connects |
| `CONNECT A B` | Start outbound link; uses RETRIES |
| `DISCONNECT` / `ABORT` | Graceful / immediate drop |
| `BW500` / `BW2300` / `BW2750` | Stored for `CONNECTED …` width reporting |
| `CLEANTXBUFFER` | `BUFFEREMPTY` or `FAILED ` |
| `IAMALIVE` | Emitted about every 60s while a host is attached |

One TCP client per command port and one per data port (second connection refused while busy). Dropping either side closes the pair.

## Gaps vs xara

- No VARA PHY/ARQ on the air — RF is modem73-only between peers that speak this bridge.
- No registered-speed tables, CQFRAME air path, digipeat, or VARA FM width negotiation beyond reporting `NARROW`/`WIDE`/`Hz` tokens hosts already parse.
- Data reliability is modem73 frame delivery (CSMA/fragmentation as configured), not VARA’s ARQ levels/`BITRATE` adaptation.
