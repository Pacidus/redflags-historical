#!/usr/bin/env python3
import requests
import json
from datetime import datetime
import time
from pathlib import Path
import argparse
from collections import defaultdict


def main():
    parser = argparse.ArgumentParser(description="Wayback Machine JSON Downloader")
    parser.add_argument(
        "--start-date", default="2020-01-01", help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument("--end-date", default=None, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default="json_files", help="Output directory")
    parser.add_argument("--dry-run", action="store_true", help="Just show snapshots")
    parser.add_argument(
        "--delay", type=float, default=1.0, help="Delay between downloads"
    )
    args = parser.parse_args()

    # Setup output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)

    # Forbes API endpoints
    forbes_urls = [
        "https://www.forbes.com/forbesapi/person/rtb/0/position/true.json",
        "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json",
        "https://www.forbes.com/forbesapi/person/rtb/0/.json",
        "https://www.forbes.com/forbesapi/person/rtb/0/-estWorthPrev/true.json?fields=rank,uri,personName,lastName,gender,source,industries,countryOfCitizenship,birthDate,finalWorth,estWorthPrev,imageExists,squareImage,listUri",
    ]

    # Setup session
    session = requests.Session()
    session.headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N)"
    }

    # Get available snapshots
    print(f"🔍 Searching snapshots from {args.start_date} to {args.end_date or 'now'}")
    snapshots = []
    timestamp_counts = defaultdict(int)

    for i, url in enumerate(forbes_urls, 1):
        print(f"📡 Checking URL {i}/{len(forbes_urls)}")
        params = {
            "url": url,
            "output": "json",
            "from": args.start_date.replace("-", ""),
            "to": (args.end_date or datetime.now().strftime("%Y-%m-%d")).replace(
                "-", ""
            ),
            "filter": ["statuscode:200", "mimetype:application/json"],
            "collapse": "timestamp:8",
        }

        try:
            res = session.get(
                "https://web.archive.org/cdx/search/cdx", params=params, timeout=30
            )
            res.raise_for_status()
            data = res.json()
            if len(data) > 1:
                headers = data[0]
                for row in data[1:]:
                    snap = dict(zip(headers, row))
                    snap["source_index"] = i - 1
                    snapshots.append(snap)
                print(f"   ✅ Found {len(data)-1} snapshots")
        except Exception as e:
            print(f"   ❌ Failed: {e}")

    if not snapshots:
        print("❌ No snapshots found")
        return False

    print(f"✅ Total snapshots: {len(snapshots)}")

    if args.dry_run:
        print("\n📋 First 10 snapshots:")
        for snap in snapshots[:10]:
            date_str = datetime.strptime(snap["timestamp"][:8], "%Y%m%d").strftime(
                "%Y-%m-%d"
            )
            print(f"  📅 {snap['timestamp']} ({date_str})")
        if len(snapshots) > 10:
            print(f"  ... and {len(snapshots)-10} more")
        return True

    # Download files
    print(f"📥 Downloading {len(snapshots)} files to {output_dir}/")
    successful = failed = 0

    for i, snap in enumerate(snapshots, 1):
        ts = snap["timestamp"]
        src_idx = snap["source_index"]

        # Handle duplicate timestamps
        timestamp_counts[ts] += 1
        filename = (
            f"{ts}.json" if timestamp_counts[ts] == 1 else f"{ts}_source{src_idx}.json"
        )
        filepath = output_dir / filename

        if filepath.exists():
            print(f"⏭️  Skipping {filename} (exists)")
            continue

        try:
            wayback_url = f"https://web.archive.org/web/{ts}id_/{snap['original']}"
            print(f"📥 Downloading {filename}...", end="", flush=True)

            res = session.get(wayback_url, timeout=30)
            res.raise_for_status()
            json_data = res.json()

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False)

            print(" ✅")
            successful += 1
        except Exception as e:
            print(f" ❌ Failed: {str(e)[:50]}")
            failed += 1

        # Progress updates
        if i % 10 == 0 or i == len(snapshots):
            print(f"📊 Processed: {i}/{len(snapshots)} | ✅ {successful} | ❌ {failed}")

        time.sleep(args.delay)

    print(f"\n🎉 Download completed: ✅ {successful} | ❌ {failed}")
    print(f"📁 Files saved to: {output_dir.absolute()}")
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
