#!/usr/bin/env python3
"""
parse_logs.py — Cazyx.io Master Log Parser
==========================================
Converts raw MetaTrader 5 log files (UTF-16 .log or UTF-8 .csv) into a
single clean normalized trades.json file for the cazyx.io dashboard.

REPEATABLE WORKFLOW:
  1. User sends a new log file (e.g. 20260327.log)
  2. Run:  python3 parse_logs.py /path/to/20260327.log
  3. Script appends new trades, updates open positions, regenerates trades.json
  4. Push trades.json to GitHub → site updates immediately

FULL REBUILD (all logs):
  python3 parse_logs.py --all /path/to/logs/directory/

OUTPUT: trades.json in the current directory (or --output path)

Schema:
  {
    "generated": "2026-03-26T18:52:11",
    "lastLogDate": "20260326",
    "lastLogTimestamp": "2026.03.26 18:52:11",
    "trades": [ TradeRecord, ... ],
    "openPositions": [ OpenPosition, ... ],
    "dailySummaries": [ DailySummary, ... ],
    "metadata": { "totalLogs": 21, "totalTrades": 15, ... }
  }

TradeRecord fields:
  date, time, symbol, direction, entryPrice, exitPrice, slPrice, tpPrice,
  lots, profitUSD, mfePips, maePips, durationMin, result, exitReason,
  entryType, version, ticket

OpenPosition fields:
  ticket, symbol, direction, entryPrice, currentSL, tpPrice, lots,
  openDate, openTime, entryType, version, unrealizedPnL, currentR, mfePips, maePips

DailySummary fields:
  date, version, barsEvaluated, signalsFired, ordersPlaced, slHits,
  limitPlaced, limitFilled, limitExpired, fallbackOrders,
  gateFailDir, gateFailSpread, gateFailATR, gateFailTouch, gateFailMom,
  gateFailZeroRange, gateFailCooldown, gateFailADX, gateFailDailyTF,
  lotCapped, equity, balance, dayPnL
"""

import re
import sys
import json
import os
import argparse
from datetime import datetime
from typing import Optional


# ─── Version detection ────────────────────────────────────────────────────────

def get_version(date: str) -> str:
    d = int(date)
    if d <= 20260304: return "v4.0"
    if d <= 20260309: return "v4.1"
    if d <= 20260312: return "v4.2"
    if d <= 20260320: return "v4.3"
    if d <= 20260324: return "v4.4"
    if d <= 20260331: return "v4.5"
    return "v4.6"


# ─── File loading ─────────────────────────────────────────────────────────────

def load_lines(path: str) -> list[str]:
    """Load a log file, auto-detecting UTF-16 or UTF-8 encoding."""
    for enc in ("utf-16", "utf-8", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                content = f.read()
            return [l.strip() for l in content.splitlines() if l.strip()]
        except (UnicodeDecodeError, UnicodeError):
            continue
    raise ValueError(f"Cannot decode {path}")


def date_from_path(path: str) -> str:
    """Extract YYYYMMDD from filename like 20260326.log or 20260326.csv"""
    name = os.path.basename(path)
    m = re.search(r'(\d{8})', name)
    if m:
        return m.group(1)
    raise ValueError(f"Cannot extract date from filename: {name}")


# ─── Field extractors ─────────────────────────────────────────────────────────

def flt(text: str, key: str) -> Optional[float]:
    """Extract float value for key= pattern."""
    m = re.search(rf'{re.escape(key)}=([+-]?\d+\.?\d*)', text, re.IGNORECASE)
    return float(m.group(1)) if m else None

def integer(text: str, key: str) -> Optional[int]:
    m = re.search(rf'{re.escape(key)}=(\d+)', text, re.IGNORECASE)
    return int(m.group(1)) if m else None

def extract_datetime(line: str) -> Optional[tuple[str, str]]:
    """Returns (YYYYMMDD, HH:MM:SS) from log line."""
    m = re.search(r'(\d{4})\.(\d{2})\.(\d{2})\s+(\d{2}:\d{2}:\d{2})', line)
    if m:
        return f"{m.group(1)}{m.group(2)}{m.group(3)}", m.group(4)
    return None

def event_type(line: str) -> Optional[str]:
    m = re.search(r'\|\s*([A-Z_]+)\s*\|', line)
    return m.group(1) if m else None

def event_symbol(line: str) -> Optional[str]:
    """Extract the symbol field (3rd pipe-delimited segment)."""
    parts = line.split('|')
    if len(parts) >= 3:
        s = parts[2].strip()
        if re.match(r'^[A-Z]{6}$', s):
            return s
    return None

def event_msg(line: str) -> str:
    """Everything after the 3rd pipe."""
    parts = line.split('|')
    return '|'.join(parts[3:]).strip() if len(parts) >= 4 else ""


# ─── Core parser ──────────────────────────────────────────────────────────────

class LogParser:
    def __init__(self):
        # Keyed by ticket string
        self.open_by_ticket: dict[str, dict] = {}
        # symbol → latest ticket (for matching closes when ticket not in close event)
        self.symbol_to_ticket: dict[str, str] = {}

        self.closed_trades: list[dict] = []
        self.daily_summaries: list[dict] = []
        self.last_timestamp: str = ""

    def parse_file(self, path: str):
        date = date_from_path(path)
        lines = load_lines(path)
        version = get_version(date)
        self._process_lines(lines, date, version)

    def _process_lines(self, lines: list[str], date: str, version: str):
        for line in lines:
            dt = extract_datetime(line)
            if dt:
                self.last_timestamp = f"{dt[0][:4]}.{dt[0][4:6]}.{dt[0][6:]} {dt[1]}"

            ev = event_type(line)
            if not ev:
                continue

            sym = event_symbol(line)
            msg = event_msg(line)

            if ev in ("ORDER_PLACED", "LIMIT_PLACED"):
                self._handle_open(line, ev, sym, msg, date, version, dt)
            elif ev == "LIMIT_FILLED":
                self._handle_fill(line, sym, msg, dt)
            elif ev == "LIMIT_EXPIRED":
                self._handle_expired(sym, msg)
            elif ev in ("SL_HIT", "TP_HIT"):
                self._handle_close(line, ev, sym, msg, dt, version)
            elif ev == "TRAIL_MOVED":
                self._handle_trail(sym, msg)
            elif ev == "POSITION_HEALTH":
                self._handle_health(sym, msg, dt)
            elif ev == "DAILY_SUMMARY":
                self._handle_summary(msg, date, version)

    def _handle_open(self, line, ev, sym, msg, date, version, dt):
        """Handle ORDER_PLACED (v4.0 market) and LIMIT_PLACED (v4.2+ limit)."""
        # Direction: BUY/SELL or BUY_LIMIT/SELL_LIMIT or FALLBACK BUY/SELL
        dir_m = re.search(r'(FALLBACK\s+)?(BUY|SELL)(?:_LIMIT)?', msg, re.IGNORECASE)
        if not dir_m:
            return
        is_fallback = bool(dir_m.group(1))
        direction = "LONG" if "BUY" in dir_m.group(2).upper() else "SHORT"
        entry_type = "FALLBACK_MARKET" if is_fallback else ("MARKET" if ev == "ORDER_PLACED" else "LIMIT")

        ticket_m = re.search(r'ticket=(\d+)', msg, re.IGNORECASE)
        ticket = ticket_m.group(1) if ticket_m else f"auto_{date}_{sym}"

        entry = flt(msg, "entry") or flt(msg, "limitPrice") or flt(msg, "openPrice")
        sl = flt(msg, "sl")
        tp = flt(msg, "tp")
        lots = flt(msg, "lots")

        rec = {
            "ticket": ticket,
            "symbol": sym,
            "direction": direction,
            "entryPrice": entry,
            "slPrice": sl,
            "tpPrice": tp,
            "lots": lots,
            "openDate": dt[0] if dt else date,
            "openTime": dt[1] if dt else "00:00:00",
            "entryType": entry_type,
            "version": version,
            "filled": entry_type in ("MARKET", "FALLBACK_MARKET"),  # market orders fill immediately
            "fillDate": dt[0] if dt and entry_type in ("MARKET", "FALLBACK_MARKET") else None,
            "fillTime": dt[1] if dt and entry_type in ("MARKET", "FALLBACK_MARKET") else None,
            "fillPrice": entry if entry_type in ("MARKET", "FALLBACK_MARKET") else None,
            "mfePips": None,
            "maePips": None,
            "unrealizedPnL": None,
            "currentR": None,
        }

        self.open_by_ticket[ticket] = rec
        if sym:
            self.symbol_to_ticket[sym] = ticket

    def _handle_fill(self, line, sym, msg, dt):
        """LIMIT_FILLED — mark the pending order as filled."""
        ticket_m = re.search(r'ticket=(\d+)', msg, re.IGNORECASE)
        ticket = ticket_m.group(1) if ticket_m else self.symbol_to_ticket.get(sym)
        if not ticket or ticket not in self.open_by_ticket:
            return
        rec = self.open_by_ticket[ticket]
        fill_price = flt(msg, "FillPrice") or flt(msg, "fillPrice")
        rec["filled"] = True
        rec["fillDate"] = dt[0] if dt else rec["openDate"]
        rec["fillTime"] = dt[1] if dt else rec["openTime"]
        rec["fillPrice"] = fill_price or rec["entryPrice"]
        if sym:
            self.symbol_to_ticket[sym] = ticket

    def _handle_expired(self, sym, msg):
        """LIMIT_EXPIRED — remove from open positions."""
        ticket_m = re.search(r'ticket=(\d+)', msg, re.IGNORECASE)
        ticket = ticket_m.group(1) if ticket_m else self.symbol_to_ticket.get(sym)
        if ticket and ticket in self.open_by_ticket:
            del self.open_by_ticket[ticket]
        if sym and self.symbol_to_ticket.get(sym) == ticket:
            del self.symbol_to_ticket[sym]

    def _handle_close(self, line, ev, sym, msg, dt, version):
        """SL_HIT or TP_HIT — close the open position and record the trade."""
        ticket_m = re.search(r'ticket=(\d+)', msg, re.IGNORECASE)
        ticket = ticket_m.group(1) if ticket_m else self.symbol_to_ticket.get(sym)

        # Find the open record
        rec = None
        if ticket and ticket in self.open_by_ticket:
            rec = self.open_by_ticket[ticket]
        elif sym and sym in self.symbol_to_ticket:
            t = self.symbol_to_ticket[sym]
            rec = self.open_by_ticket.get(t)
            ticket = t

        profit = flt(msg, "Profit") or flt(msg, "profit") or 0.0
        mfe = flt(msg, "MFE")
        mae = flt(msg, "MAE")
        duration = integer(msg, "TimeOpen") or integer(msg, "timeOpen")
        entry_price_log = flt(msg, "EntryPrice") or flt(msg, "entryPrice")

        # Exit price: use the current SL (which may have been trailed) from the open record
        if rec:
            exit_price = rec["slPrice"] if ev == "SL_HIT" else rec["tpPrice"]
            entry_price = rec.get("fillPrice") or rec.get("entryPrice") or entry_price_log
        else:
            exit_price = None
            entry_price = entry_price_log

        close_date = dt[0] if dt else "00000000"
        close_time = dt[1] if dt else "00:00:00"

        trade = {
            "ticket": ticket or "unknown",
            "date": rec["fillDate"] if rec and rec.get("fillDate") else (rec["openDate"] if rec else close_date),
            "time": rec["fillTime"] if rec and rec.get("fillTime") else (rec["openTime"] if rec else close_time),
            "closeDate": close_date,
            "closeTime": close_time,
            "symbol": sym or (rec["symbol"] if rec else "UNKNOWN"),
            "direction": rec["direction"] if rec else "LONG",
            "entryPrice": entry_price,
            "exitPrice": exit_price,
            "slPrice": rec["slPrice"] if rec else None,
            "tpPrice": rec["tpPrice"] if rec else None,
            "lots": rec["lots"] if rec else None,
            "profitUSD": round(profit, 2),
            "mfePips": mfe,
            "maePips": mae,
            "durationMin": duration,
            "result": "WIN" if profit > 0 else "LOSS",
            "exitReason": ev,
            "entryType": rec["entryType"] if rec else "UNKNOWN",
            "version": rec["version"] if rec else version,
        }

        self.closed_trades.append(trade)

        # Remove from open positions
        if ticket and ticket in self.open_by_ticket:
            del self.open_by_ticket[ticket]
        if sym and self.symbol_to_ticket.get(sym) == ticket:
            del self.symbol_to_ticket[sym]

    def _handle_trail(self, sym, msg):
        """TRAIL_MOVED — update the current SL on the open position."""
        ticket_m = re.search(r'ticket=(\d+)', msg, re.IGNORECASE)
        ticket = ticket_m.group(1) if ticket_m else self.symbol_to_ticket.get(sym)
        if not ticket or ticket not in self.open_by_ticket:
            return
        new_sl = flt(msg, "newSL") or flt(msg, "NewSL") or flt(msg, "sl")
        if new_sl:
            self.open_by_ticket[ticket]["slPrice"] = new_sl

    def _handle_health(self, sym, msg, dt):
        """POSITION_HEALTH — update unrealized P&L and excursion data."""
        ticket_m = re.search(r'ticket=(\d+)', msg, re.IGNORECASE)
        ticket = ticket_m.group(1) if ticket_m else self.symbol_to_ticket.get(sym)
        if not ticket or ticket not in self.open_by_ticket:
            return
        rec = self.open_by_ticket[ticket]
        # Update trailing SL from health report
        sl = flt(msg, "SL") or flt(msg, "sl")
        if sl:
            rec["slPrice"] = sl
        rec["unrealizedPnL"] = flt(msg, "unrealPnL")
        rec["currentR"] = flt(msg, "curR")
        rec["mfePips"] = flt(msg, "MFE")
        rec["maePips"] = flt(msg, "MAE")

    def _handle_summary(self, msg, date, version):
        """DAILY_SUMMARY — extract session-end statistics."""
        s = {
            "date": date,
            "version": version,
            "barsEvaluated": integer(msg, "Bars") or 0,
            "signalsFired": integer(msg, "Signals") or 0,
            "ordersPlaced": integer(msg, "Orders") or 0,
            "slHits": integer(msg, "SLHits") or 0,
            "limitPlaced": integer(msg, "LimitPlaced") or 0,
            "limitFilled": integer(msg, "LimitFilled") or 0,
            "limitExpired": integer(msg, "LimitExpired") or 0,
            "fallbackOrders": integer(msg, "FallbackOrders") or 0,
            "gateFailDir": integer(msg, "DIR") or 0,
            "gateFailSpread": integer(msg, "SPR") or 0,
            "gateFailATR": integer(msg, "ATR") or 0,
            "gateFailTouch": integer(msg, "TOUCH") or 0,
            "gateFailMom": integer(msg, "MOM") or 0,
            "gateFailZeroRange": integer(msg, "ZRNG") or 0,
            "gateFailCooldown": integer(msg, "COOL") or 0,
            "gateFailADX": integer(msg, "ADX") or 0,
            "gateFailDailyTF": integer(msg, "DAILY_TF") or 0,
            "lotCapped": integer(msg, "LotCapped") or 0,
            "equity": flt(msg, "Equity") or 0.0,
            "balance": flt(msg, "Balance") or 0.0,
            "dayPnL": flt(msg, "DayPnL") or 0.0,
        }
        self.daily_summaries.append(s)

    def get_open_positions(self, most_recent_date: str) -> list[dict]:
        """Return only positions that are filled and not stale."""
        result = []
        recent_num = int(most_recent_date)
        for ticket, rec in self.open_by_ticket.items():
            if not rec.get("filled"):
                continue
            pos_date = int(rec.get("fillDate") or rec.get("openDate") or "0")
            if recent_num - pos_date > 2:
                continue  # stale — skip
            result.append({
                "ticket": ticket,
                "symbol": rec["symbol"],
                "direction": rec["direction"],
                "entryPrice": rec.get("fillPrice") or rec.get("entryPrice"),
                "currentSL": rec["slPrice"],
                "tpPrice": rec["tpPrice"],
                "lots": rec["lots"],
                "openDate": rec.get("fillDate") or rec.get("openDate"),
                "openTime": rec.get("fillTime") or rec.get("openTime"),
                "entryType": rec["entryType"],
                "version": rec["version"],
                "unrealizedPnL": rec.get("unrealizedPnL"),
                "currentR": rec.get("currentR"),
                "mfePips": rec.get("mfePips"),
                "maePips": rec.get("maePips"),
            })
        return result

    def to_json(self, most_recent_date: str) -> dict:
        open_pos = self.get_open_positions(most_recent_date)
        total_trades = len(self.closed_trades)
        wins = sum(1 for t in self.closed_trades if t["result"] == "WIN")
        total_pnl = sum(t["profitUSD"] for t in self.closed_trades)

        return {
            "generated": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "lastLogDate": most_recent_date,
            "lastLogTimestamp": self.last_timestamp,
            "trades": self.closed_trades,
            "openPositions": open_pos,
            "dailySummaries": self.daily_summaries,
            "metadata": {
                "totalLogs": len(self.daily_summaries),
                "totalTrades": total_trades,
                "wins": wins,
                "losses": total_trades - wins,
                "winRate": round(wins / total_trades * 100, 1) if total_trades else 0,
                "totalPnL": round(total_pnl, 2),
            }
        }


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Cazyx.io log parser")
    ap.add_argument("inputs", nargs="+", help="Log file(s) or directory (with --all)")
    ap.add_argument("--all", action="store_true", help="Process all .log/.csv files in the given directory")
    ap.add_argument("--output", default="trades.json", help="Output JSON file path")
    ap.add_argument("--existing", help="Path to existing trades.json to merge into (for incremental updates)")
    args = ap.parse_args()

    parser = LogParser()

    # If merging into existing data, pre-load closed trades from it
    if args.existing and os.path.exists(args.existing):
        with open(args.existing) as f:
            existing = json.load(f)
        # Load existing closed trades (keyed by ticket to avoid duplication)
        existing_tickets = {t["ticket"] for t in existing.get("trades", [])}
        parser.closed_trades = existing.get("trades", [])
        parser.daily_summaries = existing.get("dailySummaries", [])
        print(f"Loaded {len(parser.closed_trades)} existing trades from {args.existing}")
    else:
        existing_tickets = set()

    # Collect files to process
    files = []
    if args.all:
        for inp in args.inputs:
            if os.path.isdir(inp):
                for fn in sorted(os.listdir(inp)):
                    if re.match(r'\d{8}\.(log|csv)$', fn):
                        files.append(os.path.join(inp, fn))
            else:
                files.append(inp)
    else:
        files = args.inputs

    files = sorted(files, key=lambda p: re.search(r'\d{8}', os.path.basename(p)).group() if re.search(r'\d{8}', os.path.basename(p)) else "0")

    most_recent_date = "20260101"
    for path in files:
        if not os.path.exists(path):
            print(f"WARNING: {path} not found, skipping")
            continue
        try:
            date = date_from_path(path)
            print(f"Processing {os.path.basename(path)} ({get_version(date)})...")
            parser.parse_file(path)
            if date > most_recent_date:
                most_recent_date = date
        except Exception as e:
            print(f"ERROR processing {path}: {e}")

    output = parser.to_json(most_recent_date)

    with open(args.output, "w") as f:
        json.dump(output, f, indent=2)

    m = output["metadata"]
    print(f"\n✓ Done. Output: {args.output}")
    print(f"  Trades: {m['totalTrades']} ({m['wins']}W / {m['losses']}L, {m['winRate']}% win rate)")
    print(f"  Net P&L: ${m['totalPnL']:+.2f}")
    print(f"  Open positions: {len(output['openPositions'])}")
    print(f"  Daily summaries: {len(output['dailySummaries'])}")
    print(f"  Last log: {output['lastLogTimestamp']}")


if __name__ == "__main__":
    main()
