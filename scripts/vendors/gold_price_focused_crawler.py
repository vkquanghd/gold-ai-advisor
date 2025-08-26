#!/usr/bin/env python3
"""
Focused Vietnamese Gold Price Crawler
Targets specific gold price APIs discovered from CafeF.vn

Features:
- CLI: --out (full path, no extension) OR (--outdir + --basename)
- --days to keep only last N days after parsing (default 95)
- Auto-create output directory
- Normalize timestamps to naive UTC for safe comparisons
- Save CSV & JSON with timestamped filenames
- Copy latest JSON to data/vn_raw.json (or <out>.json if --out provided)
- Exit non-zero if no valid records (for Makefile/CI)

Usage examples:
    python scripts/vendors/gold_price_focused_crawler.py --outdir data --basename vn_prices --days 95
    python scripts/vendors/gold_price_focused_crawler.py --out data/vn_prices_latest --days 60
"""

import os
import sys
import re
import json
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple


# --------------------- Time helpers ---------------------
def to_naive(dt_or_str: Any) -> Optional[datetime]:
    """
    Return a naive datetime (no tzinfo). If input is aware, convert to UTC then drop tzinfo.
    Accepts ISO string or datetime object.
    """
    if dt_or_str is None:
        return None
    if isinstance(dt_or_str, str):
        # Normalize common ISO variants
        s = dt_or_str.strip().replace("Z", "+00:00")
        try:
            d = datetime.fromisoformat(s)
        except Exception:
            return None
    elif isinstance(dt_or_str, datetime):
        d = dt_or_str
    else:
        return None

    if d.tzinfo is not None:
        d = d.astimezone(timezone.utc).replace(tzinfo=None)  # UTC naive
    else:
        # Treat as naive already
        pass
    return d


def parse_date_any(val: Any) -> Optional[datetime]:
    """
    Parse many possible date/time formats and always return a naive datetime (UTC).
    """
    if val is None:
        return None
    try:
        if isinstance(val, str):
            s = val.strip()
            # ISO with 'T' or 'Z'
            if "T" in s or s.endswith("Z"):
                return to_naive(s)
            # "YYYY-MM-DD HH:MM:SS"
            if " " in s and re.match(r"\d{4}-\d{2}-\d{2}\s+\d{2}:", s):
                d = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
                return to_naive(d.replace(tzinfo=timezone.utc))
            # "YYYY-MM-DD"
            if re.match(r"\d{4}-\d{2}-\d{2}$", s):
                d = datetime.strptime(s, "%Y-%m-%d")
                # treat as midnight UTC
                return to_naive(d.replace(tzinfo=timezone.utc))
            # "MM/DD/YYYY"
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}$", s):
                d = datetime.strptime(s, "%m/%d/%Y")
                return to_naive(d.replace(tzinfo=timezone.utc))
        elif isinstance(val, (int, float)):
            # epoch seconds or ms
            if val > 1_000_000_000_000:  # ms
                d = datetime.fromtimestamp(val / 1000.0, tz=timezone.utc)
            else:
                d = datetime.fromtimestamp(val, tz=timezone.utc)
            return to_naive(d)
    except Exception:
        return None
    return None


class FocusedGoldPriceCrawler:
    def __init__(self):
        self.base_url = "https://cafef.vn"
        self.headers = {
            'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/91.0.4472.124 Safari/537.36'),
            'Referer': 'https://cafef.vn/du-lieu/gia-vang-hom-nay/trong-nuoc.chn',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'vi-VN,vi;q=0.9,en;q=0.8',
            'X-Requested-With': 'XMLHttpRequest',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
        }

        # Known CafeF endpoints
        self.gold_apis = [
            "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=1m",
            "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=3m",
            "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=6m",
            "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=1y",
            "https://cafef.vn/du-lieu/Ajax/ajaxgoldpricehistory.ashx?index=2y",
            "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=1m&zone=11",
            "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=3m&zone=11",
            "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=6m&zone=11",
            "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=1y&zone=11",
            "https://cafef.vn/du-lieu/Ajax/AjaxGoldPriceRing.ashx?time=2y&zone=11",
            "https://cafef.vn/du-lieu/Ajax/ajaxgoldprice.ashx?index=11",
            "https://cafef.vn/du-lieu/Ajax/ajaxgoldprice.ashx?index=12",
        ]

    # --------------------- HTTP helpers ---------------------
    def _get(self, url: str) -> Optional[requests.Response]:
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            if resp.status_code == 200:
                return resp
            print(f"✗ HTTP {resp.status_code} from {url}")
        except Exception as e:
            print(f"✗ Error GET {url}: {e}")
        return None

    def fetch_gold_data(self) -> List[Dict[str, Any]]:
        """Fetch data from gold price APIs; returns list of JSON-like dicts."""
        all_data: List[Dict[str, Any]] = []
        successful_apis: List[str] = []

        for api_url in self.gold_apis:
            print(f"Trying API: {api_url}")
            resp = self._get(api_url)
            if not resp:
                continue

            # Try JSON parse first
            try:
                data = resp.json()
                if isinstance(data, list) and data:
                    print(f"✓ Success: {len(data)} records from {api_url}")
                    all_data.extend(data)
                    successful_apis.append(api_url)
                elif isinstance(data, dict) and data:
                    print(f"✓ Success: JSON object from {api_url}")
                    all_data.append(data)
                    successful_apis.append(api_url)
                else:
                    print(f"✗ Empty/unknown JSON from {api_url}")
                    continue
            except json.JSONDecodeError:
                # Try to extract JSON blobs from HTML/JS
                content = resp.text or ""
                if len(content) < 50:
                    print(f"✗ Non-JSON tiny content from {api_url}")
                    continue
                print(f"Got non-JSON response from {api_url}: {content[:200]}...")

                patterns = [
                    r'data\s*[:=]\s*(\[.*?\])',
                    r'result\s*[:=]\s*(\[.*?\])',
                    r'(\[.*?\])',
                    r'(\{.*?\})',
                ]
                extracted_any = False
                for pattern in patterns:
                    matches = re.findall(pattern, content, re.DOTALL)
                    for m in matches:
                        try:
                            obj = json.loads(m)
                            if isinstance(obj, list) and obj:
                                print(f"✓ Extracted {len(obj)} records from {api_url}")
                                all_data.extend(obj)
                                successful_apis.append(api_url)
                                extracted_any = True
                                break
                            elif isinstance(obj, dict) and obj:
                                print(f"✓ Extracted object from {api_url}")
                                all_data.append(obj)
                                successful_apis.append(api_url)
                                extracted_any = True
                                break
                        except Exception:
                            continue
                    if extracted_any:
                        break
                if not extracted_any:
                    print(f"✗ Could not extract JSON from {api_url}")

        print(f"\nSuccessful APIs: {len(successful_apis)}")
        for api in successful_apis:
            print(f"  - {api}")
        return all_data

    # --------------------- Parsing ---------------------
    def parse_gold_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Normalize to list of:
        {date,time,timestamp,gold_type,buy_price,sell_price}
        """
        parsed: List[Dict[str, Any]] = []

        for api_response in raw_data:
            if not isinstance(api_response, dict):
                continue

            candidates: List[Any] = []

            if "Data" in api_response and isinstance(api_response["Data"], dict):
                data_obj = api_response["Data"]
                if "goldPriceWorldHistories" in data_obj and isinstance(data_obj["goldPriceWorldHistories"], list):
                    candidates = data_obj["goldPriceWorldHistories"]

            if not candidates and "goldPriceWorldHistories" in api_response:
                if isinstance(api_response["goldPriceWorldHistories"], list):
                    candidates = api_response["goldPriceWorldHistories"]

            # Try any list-of-dict under unexpected keys
            if not candidates and api_response:
                for k, v in api_response.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        candidates = v
                        break

            if not candidates:
                continue

            print(f"Found {len(candidates)} historical items in response")

            for item in candidates:
                if not isinstance(item, dict):
                    continue

                # Date/time
                date_val = None
                for key in ["createdAt", "lastUpdated", "Date", "CreatedAt", "Time", "timestamp"]:
                    if key in item:
                        date_val = item[key]
                        break
                dt_obj = parse_date_any(date_val) or to_naive(datetime.utcnow())

                # Prices
                buy_price, sell_price = None, None
                name = item.get("name", "Unknown")

                for f in ["buyPrice", "BuyPrice", "GiaMua", "Buy", "mua"]:
                    if f in item and item[f] is not None:
                        try:
                            buy_price = float(item[f])
                            break
                        except Exception:
                            pass

                for f in ["sellPrice", "SellPrice", "GiaBan", "Sell", "ban"]:
                    if f in item and item[f] is not None:
                        try:
                            sell_price = float(item[f])
                            break
                        except Exception:
                            pass

                # Heuristic unit normalization (simplified)
                if name == "SJC" and buy_price and buy_price < 200_000:
                    buy_price *= 1_000_000
                    if sell_price:
                        sell_price *= 1_000_000

                parsed.append({
                    "date": dt_obj.strftime("%Y-%m-%d"),
                    "time": dt_obj.strftime("%H:%M:%S"),
                    "timestamp": dt_obj.isoformat(),
                    "gold_type": name,
                    "buy_price": float(buy_price or 0),
                    "sell_price": float(sell_price or 0),
                })

        # Deduplicate
        seen = set()
        unique: List[Dict[str, Any]] = []
        for it in parsed:
            key = (it["date"], it["time"], it["gold_type"], it["buy_price"], it["sell_price"])
            if key not in seen:
                seen.add(key)
                unique.append(it)

        # Sort by naive timestamp ascending
        unique.sort(key=lambda x: to_naive(x["timestamp"]) or datetime.min)
        return unique

    # --------------------- Save & summary ---------------------
    @staticmethod
    def save_data(data: List[Dict[str, Any]], outdir: Path, basename: str) -> Tuple[Path, Path]:
        outdir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = outdir / f"{basename}_{ts}.csv"
        json_path = outdir / f"{basename}_{ts}.json"

        rows = []
        for it in data:
            rows.append({
                "Date": it["date"],
                "Time": it["time"],
                "Gold Type": it["gold_type"],
                "Buy Price (VND)": it["buy_price"],
                "Sell Price (VND)": it["sell_price"],
                "Timestamp": it["timestamp"],
            })
        pd.DataFrame(rows).to_csv(csv_path, index=False, encoding="utf-8-sig")
        json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n✓ Saved CSV:  {csv_path}")
        print(f"✓ Saved JSON: {json_path}")
        return csv_path, json_path

    @staticmethod
    def display_summary(data: List[Dict[str, Any]]):
        if not data:
            print("No data to summarize")
            return
        print("\n" + "=" * 50)
        print("VIETNAMESE GOLD PRICE DATA SUMMARY")
        print("=" * 50)
        print(f"Total records: {len(data)}")
        print(f"Date range: {data[0]['date']} → {data[-1]['date']}")
        buy_cnt = sum(1 for x in data if x["buy_price"] > 0)
        sell_cnt = sum(1 for x in data if x["sell_price"] > 0)
        print(f"Records with buy:  {buy_cnt}")
        print(f"Records with sell: {sell_cnt}")

        types = sorted(set(x["gold_type"] for x in data))
        print(f"Gold types: {', '.join(types)}")
        print("\nRecent 5 records:")
        for x in data[-5:]:
            print(f"  {x['date']} {x['time']} | {x['gold_type']} | "
                  f"Buy={x['buy_price']:,.0f} | Sell={x['sell_price']:,.0f}")

    # --------------------- Main crawl ---------------------
    def crawl(self, args) -> Tuple[Optional[Path], Optional[Path]]:
        print("🏆 Vietnamese Gold Price Focused Crawler")
        print("=" * 50)

        raw = self.fetch_gold_data()
        if not raw:
            print("❌ No data retrieved from any API")
            return None, None

        print(f"\n📊 Retrieved {len(raw)} raw items (objects/lists)")
        parsed = self.parse_gold_data(raw)
        if not parsed:
            print("❌ No valid gold price data found after parsing")
            return None, None

        # Filter last N days
        if args.days and args.days > 0:
            cutoff = datetime.utcnow().replace(tzinfo=None) - timedelta(days=args.days)  # naive UTC
            filtered = []
            for x in parsed:
                dt_naive = to_naive(x["timestamp"])
                if dt_naive and dt_naive >= cutoff:
                    filtered.append(x)
            parsed = filtered
            print(f"ℹ Filtered to last {args.days} days → {len(parsed)} records")

        if not parsed:
            print("❌ No data left after filtering")
            return None, None

        # Resolve output paths
        if args.out:
            out_path = Path(args.out)
            outdir = out_path.parent
            basename = out_path.stem
        else:
            outdir = Path(args.outdir or "data")
            basename = args.basename or "vietnamese_gold_prices"

        csv_path, json_path = self.save_data(parsed, outdir, basename)
        self.display_summary(parsed)
        print("\n🎉 Crawling complete!")
        return csv_path, json_path


# --------------------- CLI ---------------------
def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser()
    p.add_argument("--out", help="Full output file path without extension. Overrides outdir/basename if set.")
    p.add_argument("--outdir", default="data", help="Output directory (used if --out is not set).")
    p.add_argument("--basename", default="vietnamese_gold_prices", help="Base filename (used if --out is not set).")
    p.add_argument("--days", type=int, default=365, help="Keep only last N days after parsing.")
    return p


def main():
    args = build_argparser().parse_args()
    crawler = FocusedGoldPriceCrawler()
    csv_path, json_path = crawler.crawl(args)
    if not json_path:
        sys.exit(2)  # non-zero => fail Makefile target

    # Also write a fixed/latest copy for importer convenience
    if args.out:
        latest_json = Path(args.out).with_suffix(".json")
    else:
        latest_json = Path(args.outdir or "data") / "vn_raw.json"
    try:
        latest_json.parent.mkdir(parents=True, exist_ok=True)
        content = Path(json_path).read_text(encoding="utf-8")
        latest_json.write_text(content, encoding="utf-8")
        print(f"✓ Latest JSON copied to: {latest_json}")
    except Exception as e:
        print(f"⚠ Could not write latest JSON: {e}")

if __name__ == "__main__":
    main()