# Port of scam_pump_liqs' `examples/config/config.nix` + `examples/situations.nix`, restricted to
# what this reimplementation actually has. SPL keeps the two apart because it runs many situations
# against one config; this example runs exactly one, so the situation lives here.
#
# The values below are the MOCK example's: `screen.Std`, `use_4h = false`, and the PROVE-USDT window
# `examples/situations.nix` points at.
{
  # The one backtest target. Days before `start` are warmup, consumed silently; `end` is exclusive.
  # `coingecko_id` keys the market-cap lane.
  situation = {
    pair = "PROVE-USDT";
    coingecko_id = "succinct";
    start = "2026-05-20";
    end = "2026-05-22";
  };

  strategy = {
    indies = {
      rsi = { timeframe = "5m"; base_len = 14; smooth_len = 14; };
      momentum = { fast = "5m"; risk_free_rate = 0.04; };
      atr = { period = 14; };
    };

    # Exactly one screener runs, and its indie is what joins the universal set in the shallow
    # snapshot's required fields. The other is inert.
    screen = {
      #Rsi = {
      #  rsi_threshold = 65.0;
      #  price_percent = "20%";
      #};
      Std = {
        fast_overvalued = 5.0;
      };
    };

    classification = {
      max_size = 20000.0;
      drain_grace = "1m";
      liquidations = {
        atr_sl_x = 2.5;
        atr_tp_x = 5.0;
        trail_pct = "2%";
        trail_severity = "33%";
      };
    };
  };

  backtest = {
    # SPL's `backtest.latency` is *order-command* latency for its simulated venue — execution, which
    # this port stops short of. What a `Replay` needs instead is the arrival latency it hangs on rows
    # that carry no recorded reception, so that is what this is. Same shape, different quantity.
    arrival_latency = { p68 = "25ms"; p95 = "60ms"; p997 = "150ms"; };

    # How coarsely arrival time is cut before the graph is stepped over it. Everything landing in one
    # cell is handed over as a single tick; the reader never waits for a cell to fill.
    read_clock = "100ms";
  };
}
