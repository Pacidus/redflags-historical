#!/usr/bin/env python3
import json
import csv
import sys
from pathlib import Path
import argparse


def convert_json_to_csv(json_folder, output_prefix="raw_data"):
    json_files = sorted(Path(json_folder).glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {json_folder}")
        return False

    billionaires = []
    assets = []
    total_files = len(json_files)

    print(f"Processing {total_files} JSON files...")

    for i, json_file in enumerate(json_files, 1):
        if i % 10 == 0 or i == total_files:
            print(f"Processed {i}/{total_files} files...")

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            date_str = json_file.stem[:8]
            records = (
                data.get("personList", {}).get("personsLists")
                or data.get("personList")
                or data.get("data", [])
            )

            for record in records:
                billionaire = {
                    "date": date_str,
                    "personName": record.get("personName", ""),
                    "lastName": record.get("lastName", ""),
                    "birthDate": record.get("birthDate", ""),
                    "gender": record.get("gender", ""),
                    "countryOfCitizenship": record.get("countryOfCitizenship", ""),
                    "city": record.get("city", ""),
                    "state": record.get("state", ""),
                    "finalWorth": record.get("finalWorth", ""),
                    "estWorthPrev": record.get("estWorthPrev", ""),
                    "archivedWorth": record.get("archivedWorth", ""),
                    "privateAssetsWorth": record.get("privateAssetsWorth", ""),
                    "source": record.get("source", ""),
                    "industries": record.get("industries", ""),
                }
                billionaires.append(billionaire)

                # Process assets without asset_index
                for asset in record.get("financialAssets", []):
                    assets.append(
                        {
                            "date": date_str,
                            "personName": record.get("personName", ""),
                            "numberOfShares": asset.get("numberOfShares", ""),
                            "sharePrice": asset.get("sharePrice", ""),
                            "exchangeRate": asset.get("exchangeRate", ""),
                            "ticker": asset.get("ticker", ""),
                            "companyName": asset.get("companyName", ""),
                            "currencyCode": asset.get("currencyCode", ""),
                            "exchange": asset.get("exchange", ""),
                            "interactive": asset.get("interactive", ""),
                        }
                    )
        except Exception:
            continue

    # Write billionaires CSV
    if billionaires:
        with open(
            f"{output_prefix}_billionaires.csv", "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=billionaires[0].keys())
            writer.writeheader()
            writer.writerows(billionaires)
        print(
            f"Created {output_prefix}_billionaires.csv with {len(billionaires)} records"
        )

    # Write assets CSV
    if assets:
        with open(
            f"{output_prefix}_assets.csv", "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=assets[0].keys())
            writer.writeheader()
            writer.writerows(assets)
        print(f"Created {output_prefix}_assets.csv with {len(assets)} records")

    return bool(billionaires or assets)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple JSON to CSV converter")
    parser.add_argument("json_folder", help="Folder containing JSON files")
    parser.add_argument(
        "--output-prefix", "-o", default="raw_data", help="Output file prefix"
    )
    args = parser.parse_args()

    success = convert_json_to_csv(args.json_folder, args.output_prefix)
    sys.exit(0 if success else 1)
