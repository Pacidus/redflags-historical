#!/usr/bin/env python3
import json
import argparse
from pathlib import Path
from collections import defaultdict
import re
from decimal import Decimal


def detect_float_artifacts(value):
    """Detect if a value shows signs of floating-point precision issues"""
    if value is None:
        return False, "null"

    str_val = str(value).strip()

    # Check for specific patterns that indicate floating-point artifacts
    artifacts = []

    # 1. Very long decimal sequences (>10 digits after decimal)
    if "." in str_val:
        decimal_part = str_val.split(".")[1]
        if len(decimal_part) > 10:
            artifacts.append(f"long_decimal_{len(decimal_part)}")

    # 2. Repeating patterns that suggest binary->decimal conversion issues
    if re.search(r"(\d)\1{4,}", str_val.replace(".", "")):
        artifacts.append("repeating_digits")

    # 3. Common floating-point precision markers
    float_markers = [
        "00000000001",  # Very small trailing 1
        "99999999999",  # Very long 9s
        "000000001",  # Shorter trailing 1
        "999999999",  # Shorter 9s
        "0000001",  # Even shorter
        "9999999",
    ]

    for marker in float_markers:
        if marker in str_val:
            artifacts.append(f"float_marker_{marker[:6]}")

    # 4. Scientific notation patterns when converted back
    try:
        float_val = float(str_val)
        # If the float representation differs significantly from string
        if abs(float_val) > 0:
            # Check if original string has way more precision than float can represent
            reconstructed = f"{float_val:.17g}"  # Max meaningful digits for float64
            if len(str_val.replace(".", "").replace("-", "")) > 17:
                artifacts.append("excessive_precision")
    except ValueError:
        pass

    # 5. Check for values that are likely results of division/calculations
    try:
        decimal_val = Decimal(str_val)

        # Common fractions that create long decimals
        common_fractions = [
            (1, 3),  # 0.333...
            (2, 3),  # 0.666...
            (1, 6),  # 0.1666...
            (1, 7),  # 0.142857...
            (1, 9),  # 0.111...
            (1, 11),  # 0.090909...
        ]

        for num, den in common_fractions:
            expected = Decimal(num) / Decimal(den)
            # Check if value is close to these common fractions
            if (
                abs(decimal_val - expected) < Decimal("0.000000001")
                and len(str_val) > 10
            ):
                artifacts.append(f"likely_fraction_{num}_{den}")

    except:
        pass

    return len(artifacts) > 0, artifacts


def analyze_precision_artifacts(json_folder):
    """Analyze JSON files for floating-point precision artifacts"""

    billionaire_fields = [
        "finalWorth",
        "estWorthPrev",
        "archivedWorth",
        "privateAssetsWorth",
    ]

    asset_fields = [
        "numberOfShares",
        "sharePrice",
        "exchangeRate",
        "exerciseOptionPrice",
        "currentPrice",
    ]

    # Track artifacts by field
    artifact_stats = defaultdict(
        lambda: {
            "total_values": 0,
            "artifact_values": 0,
            "artifact_examples": [],
            "clean_examples": [],
            "max_clean_precision": {"before": 0, "after": 0, "value": ""},
            "artifact_types": defaultdict(int),
        }
    )

    json_files = sorted(Path(json_folder).glob("*.json"))
    print(f"Analyzing {len(json_files)} JSON files for precision artifacts...")

    for i, json_file in enumerate(json_files, 1):
        if i % 50 == 0:
            print(f"Processed {i}/{len(json_files)} files...")

        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            records = (
                data.get("personList", {}).get("personsLists")
                or data.get("personList")
                or data.get("data", [])
                or [data]
            )

            for record in records:
                # Analyze billionaire fields
                for field in billionaire_fields:
                    if field in record:
                        value = record[field]
                        is_artifact, artifact_types = detect_float_artifacts(value)

                        artifact_stats[field]["total_values"] += 1

                        if is_artifact:
                            artifact_stats[field]["artifact_values"] += 1
                            if len(artifact_stats[field]["artifact_examples"]) < 5:
                                artifact_stats[field]["artifact_examples"].append(
                                    str(value)
                                )
                            for art_type in artifact_types:
                                artifact_stats[field]["artifact_types"][art_type] += 1
                        else:
                            # Track clean values for precision analysis
                            if len(artifact_stats[field]["clean_examples"]) < 5:
                                artifact_stats[field]["clean_examples"].append(
                                    str(value)
                                )

                            # Update max clean precision
                            str_val = str(value)
                            if "." in str_val:
                                before_dot, after_dot = str_val.split(".", 1)
                                before = len(before_dot.lstrip("-"))
                                after = len(after_dot.rstrip("0"))
                            else:
                                before = len(str_val.lstrip("-"))
                                after = 0

                            current_precision = before + after
                            max_precision = (
                                artifact_stats[field]["max_clean_precision"]["before"]
                                + artifact_stats[field]["max_clean_precision"]["after"]
                            )

                            if current_precision > max_precision:
                                artifact_stats[field]["max_clean_precision"] = {
                                    "before": before,
                                    "after": after,
                                    "value": str_val,
                                }

                # Analyze asset fields
                for asset in record.get("financialAssets", []):
                    for field in asset_fields:
                        if field in asset:
                            value = asset[field]
                            is_artifact, artifact_types = detect_float_artifacts(value)

                            artifact_stats[field]["total_values"] += 1

                            if is_artifact:
                                artifact_stats[field]["artifact_values"] += 1
                                if len(artifact_stats[field]["artifact_examples"]) < 5:
                                    artifact_stats[field]["artifact_examples"].append(
                                        str(value)
                                    )
                                for art_type in artifact_types:
                                    artifact_stats[field]["artifact_types"][
                                        art_type
                                    ] += 1
                            else:
                                if len(artifact_stats[field]["clean_examples"]) < 5:
                                    artifact_stats[field]["clean_examples"].append(
                                        str(value)
                                    )

                                # Update max clean precision
                                str_val = str(value)
                                if "." in str_val:
                                    before_dot, after_dot = str_val.split(".", 1)
                                    before = len(before_dot.lstrip("-"))
                                    after = len(after_dot.rstrip("0"))
                                else:
                                    before = len(str_val.lstrip("-"))
                                    after = 0

                                current_precision = before + after
                                max_precision = (
                                    artifact_stats[field]["max_clean_precision"][
                                        "before"
                                    ]
                                    + artifact_stats[field]["max_clean_precision"][
                                        "after"
                                    ]
                                )

                                if current_precision > max_precision:
                                    artifact_stats[field]["max_clean_precision"] = {
                                        "before": before,
                                        "after": after,
                                        "value": str_val,
                                    }

        except Exception as e:
            continue

    return artifact_stats


def print_artifact_analysis(stats):
    """Print detailed artifact analysis"""
    print("\n" + "=" * 100)
    print("FLOATING-POINT PRECISION ARTIFACT ANALYSIS")
    print("=" * 100)

    billionaire_fields = [
        "finalWorth",
        "estWorthPrev",
        "archivedWorth",
        "privateAssetsWorth",
    ]
    asset_fields = [
        "numberOfShares",
        "sharePrice",
        "exchangeRate",
        "exerciseOptionPrice",
        "currentPrice",
    ]

    def print_field_analysis(field, data):
        total = data["total_values"]
        artifacts = data["artifact_values"]
        clean = total - artifacts

        if total == 0:
            return

        artifact_pct = (artifacts / total) * 100

        print(f"\n📊 {field}:")
        print(f"   Total values: {total:,}")
        print(f"   Clean values: {clean:,} ({100-artifact_pct:.1f}%)")
        print(f"   Artifacts:    {artifacts:,} ({artifact_pct:.1f}%)")

        if artifacts > 0:
            print(f"   Artifact examples: {data['artifact_examples']}")
            print(f"   Artifact types: {dict(data['artifact_types'])}")

        if clean > 0:
            max_clean = data["max_clean_precision"]
            print(
                f"   Max CLEAN precision: {max_clean['before']}.{max_clean['after']} digits ({max_clean['value']})"
            )
            print(f"   Clean examples: {data['clean_examples']}")

    print("\n🏦 BILLIONAIRE FIELDS:")
    for field in billionaire_fields:
        print_field_analysis(field, stats[field])

    print("\n💰 ASSET FIELDS:")
    for field in asset_fields:
        print_field_analysis(field, stats[field])


def generate_clean_schemas(stats):
    """Generate schemas based on clean data only"""
    print("\n" + "=" * 100)
    print("RECOMMENDED SCHEMAS (BASED ON CLEAN DATA ONLY)")
    print("=" * 100)

    billionaire_fields = [
        "finalWorth",
        "estWorthPrev",
        "archivedWorth",
        "privateAssetsWorth",
    ]
    asset_fields = [
        "numberOfShares",
        "sharePrice",
        "exchangeRate",
        "exerciseOptionPrice",
        "currentPrice",
    ]

    print("\ndef get_clean_billionaires_schema():")
    print('    """Schema based on clean data (excludes floating-point artifacts)"""')
    print("    return {")
    print('        "date": pl.Date,')
    print('        "personName": pl.Utf8,')
    print('        "lastName": pl.Utf8,')
    print('        "birthDate": pl.Date,')
    print('        "gender": pl.Utf8,')
    print('        "countryOfCitizenship": pl.Utf8,')
    print('        "city": pl.Utf8,')
    print('        "state": pl.Utf8,')
    print('        "source": pl.Utf8,')
    print('        "industries": pl.Utf8,')

    for field in billionaire_fields:
        if stats[field]["total_values"] > 0:
            clean_precision = stats[field]["max_clean_precision"]
            total = clean_precision["before"] + clean_precision["after"]
            scale = clean_precision["after"]
            # Add some buffer for safety
            total = min(total + 2, 38)
            print(
                f'        "{field}": pl.Decimal(precision={total}, scale={scale}),  # Clean max: {clean_precision["value"]}'
            )

    print("    }")

    print("\ndef get_clean_assets_schema():")
    print('    """Schema based on clean data (excludes floating-point artifacts)"""')
    print("    return {")
    print('        "date": pl.Date,')
    print('        "personName": pl.Utf8,')
    print('        "ticker": pl.Utf8,')
    print('        "companyName": pl.Utf8,')
    print('        "currencyCode": pl.Utf8,')
    print('        "exchange": pl.Utf8,')
    print('        "interactive": pl.Boolean,')

    for field in asset_fields:
        if stats[field]["total_values"] > 0:
            clean_precision = stats[field]["max_clean_precision"]
            total = clean_precision["before"] + clean_precision["after"]
            scale = clean_precision["after"]
            # Add some buffer for safety
            total = min(total + 2, 38)
            print(
                f'        "{field}": pl.Decimal(precision={total}, scale={scale}),  # Clean max: {clean_precision["value"]}'
            )

    print("    }")


def main():
    parser = argparse.ArgumentParser(
        description="Detect floating-point precision artifacts in Forbes JSON files"
    )
    parser.add_argument("json_folder", help="Folder containing JSON files")
    args = parser.parse_args()

    # Analyze files
    stats = analyze_precision_artifacts(args.json_folder)

    # Print results
    print_artifact_analysis(stats)

    # Generate clean schemas
    generate_clean_schemas(stats)

    print("\n" + "=" * 100)
    print("SUMMARY RECOMMENDATIONS:")
    print("=" * 100)
    print(
        "1. Use 'clean' schemas for financial analysis (excludes precision artifacts)"
    )
    print(
        "2. Use full precision schemas for data archival (preserves all original data)"
    )
    print("3. Consider data cleaning steps to remove obvious floating-point errors")
    print(
        "4. Monitor artifact percentages - high percentages may indicate data quality issues"
    )


if __name__ == "__main__":
    main()
