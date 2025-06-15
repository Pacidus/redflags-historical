#!/usr/bin/env python3
import json
import sys
from pathlib import Path
import argparse
from collections import defaultdict, Counter


def analyze_json_files(json_folder):
    """Analyze all JSON files to discover all possible asset columns"""
    json_files = sorted(Path(json_folder).glob("*.json"))
    if not json_files:
        print(f"❌ No JSON files found in {json_folder}")
        return False

    # Track all discovered columns
    all_asset_columns = set()
    column_frequency = Counter()
    column_sample_values = defaultdict(set)
    column_data_types = defaultdict(set)

    # Track structure variations
    structure_variations = set()
    total_files = len(json_files)
    total_assets = 0
    files_with_assets = 0

    print(f"🔍 Analyzing {total_files} JSON files for asset columns...")
    print("=" * 60)

    for i, json_file in enumerate(json_files, 1):
        if i % 50 == 0 or i == total_files:
            print(f"📊 Progress: {i}/{total_files} files processed...")

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Try different possible data structures
            records = (
                data.get("personList", {}).get("personsLists")
                or data.get("personList")
                or data.get("data", [])
            )

            if not records:
                continue

            file_has_assets = False

            for record in records:
                financial_assets = record.get("financialAssets", [])

                if financial_assets:
                    file_has_assets = True

                    for asset in financial_assets:
                        if isinstance(asset, dict):
                            total_assets += 1

                            # Collect all keys from this asset
                            asset_keys = set(asset.keys())
                            all_asset_columns.update(asset_keys)
                            structure_variations.add(tuple(sorted(asset_keys)))

                            # Track frequency and sample values for each column
                            for key, value in asset.items():
                                column_frequency[key] += 1

                                # Store sample values (limit to 5 per column)
                                if len(column_sample_values[key]) < 5:
                                    column_sample_values[key].add(
                                        str(value)[:50]
                                    )  # Truncate long values

                                # Track data types
                                column_data_types[key].add(type(value).__name__)

            if file_has_assets:
                files_with_assets += 1

        except Exception as e:
            print(f"⚠️  Error processing {json_file.name}: {e}")
            continue

    print(f"\n📈 Analysis Summary:")
    print(f"   📁 Files processed: {total_files}")
    print(f"   📁 Files with assets: {files_with_assets}")
    print(f"   💰 Total assets found: {total_assets:,}")
    print(f"   📊 Unique column names: {len(all_asset_columns)}")
    print(f"   🔄 Structure variations: {len(structure_variations)}")

    print(f"\n📋 All Discovered Asset Columns:")
    print("=" * 60)

    # Sort columns by frequency (most common first)
    sorted_columns = sorted(column_frequency.items(), key=lambda x: x[1], reverse=True)

    for i, (column, frequency) in enumerate(sorted_columns, 1):
        percentage = (frequency / total_assets * 100) if total_assets > 0 else 0
        data_types = ", ".join(sorted(column_data_types[column]))

        print(f"{i:2d}. {column}")
        print(f"    📊 Frequency: {frequency:,} / {total_assets:,} ({percentage:.1f}%)")
        print(f"    🏷️  Data types: {data_types}")

        # Show sample values
        if column_sample_values[column]:
            samples = list(column_sample_values[column])[:3]  # Show first 3 samples
            samples_str = ", ".join(f'"{s}"' for s in samples)
            if len(column_sample_values[column]) > 3:
                samples_str += f" ... (+{len(column_sample_values[column])-3} more)"
            print(f"    💡 Samples: {samples_str}")
        print()

    # Compare with current extraction
    print("🔍 Comparison with Current Extraction:")
    print("=" * 60)

    current_columns = {
        "numberOfShares",
        "sharePrice",
        "exchangeRate",
        "ticker",
        "companyName",
        "currencyCode",
        "exchange",
        "interactive",
    }

    missing_in_current = all_asset_columns - current_columns
    missing_in_discovered = current_columns - all_asset_columns

    if missing_in_current:
        print("❌ Columns found in JSON but NOT in current extraction:")
        for col in sorted(missing_in_current):
            freq = column_frequency[col]
            percentage = (freq / total_assets * 100) if total_assets > 0 else 0
            print(f"   • {col} ({freq:,} occurrences, {percentage:.1f}%)")

    if missing_in_discovered:
        print("\n⚠️  Columns in current extraction but NOT found in JSON:")
        for col in sorted(missing_in_discovered):
            print(f"   • {col}")

    if not missing_in_current and not missing_in_discovered:
        print("✅ All current columns are present in discovered data!")

    # Show structure variations
    if len(structure_variations) > 1:
        print(
            f"\n🔄 Structure Variations Found ({len(structure_variations)} different patterns):"
        )
        print("=" * 60)

        for i, structure in enumerate(
            sorted(structure_variations, key=len, reverse=True), 1
        ):
            print(f"{i}. Structure with {len(structure)} columns:")
            print(f"   {', '.join(structure)}")
            print()

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Discover all possible columns in Forbes financial assets data"
    )
    parser.add_argument("json_folder", help="Folder containing JSON files to analyze")
    parser.add_argument("--output", help="Save column list to file (optional)")

    args = parser.parse_args()

    if not Path(args.json_folder).exists():
        print(f"❌ Folder not found: {args.json_folder}")
        return False

    success = analyze_json_files(args.json_folder)

    if success and args.output:
        print(f"\n💾 Column analysis saved to: {args.output}")

    return success


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
