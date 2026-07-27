# trading_data_core

## Timestamps

Every timestamp is a pair: **who read the clock**, and **what they were doing when they read it**.

```
ts_<actor>_<action>
```

| | `exec` — acted | `send` — put on wire | `recv` — took off wire |
|---|---|---|---|
| **local** | `ts_local_exec` | `ts_local_send` | `ts_local_recv` |
| **venue** | `ts_venue_exec` | `ts_venue_send` | `ts_venue_recv` |
| **vendor** | — | `ts_vendor_send` | `ts_vendor_recv` |

Direction is not in the name — it is the order readings appear in the chain:

- outbound (order request): `local_exec → local_send → venue_recv → venue_exec`
- inbound (stream event): `venue_exec → venue_send → local_recv`
- relayed (market data vendor): `venue_exec → venue_send → vendor_recv → vendor_send → local_recv`

### Wire mapping

| Venue field | Reading |
|---|---|
| Binance `T`, Bybit `updatedTime`, BitMEX `transactTime`, OKX `uTime`, Deribit `last_update_timestamp` | `ts_venue_exec` |
| Binance `E`, Bybit envelope `creationTime`, Coinbase envelope `timestamp` | `ts_venue_send` |
| Deribit `usOut`, Kraken `time_out`, Bybit `header.Timenow` | `ts_venue_send` (request leg) |
| Deribit `usIn`, Kraken `time_in` | `ts_venue_recv` |
| Databento `hd.ts_event` | `ts_venue_exec` |
| Databento `ts_recv` | `ts_vendor_recv` |
| Databento `ts_in_delta` | derives `ts_venue_send` |
| Binance `O`/`workingTime`, Bybit `createdTime` | milestone, not a chain reading |
