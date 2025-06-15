#!/usr/bin/env python3
import polars as pl
import argparse
from pathlib import Path
import sys


def get_billionaires_csv_schema():
    """Schema for reading billionaires CSV - keep numeric fields as strings"""
    return {
        "date": pl.Utf8,
        "personName": pl.Utf8,
        "lastName": pl.Utf8,
        "birthDate": pl.Utf8,
        "gender": pl.Utf8,
        "countryOfCitizenship": pl.Utf8,
        "city": pl.Utf8,
        "state": pl.Utf8,
        "finalWorth": pl.Utf8,  # Keep as string to preserve precision
        "estWorthPrev": pl.Utf8,  # Keep as string to preserve precision
        "archivedWorth": pl.Utf8,  # Keep as string to preserve precision
        "privateAssetsWorth": pl.Utf8,  # Keep as string to preserve precision
        "source": pl.Utf8,
        "industries": pl.Utf8,
    }


def get_assets_csv_schema():
    """Schema for reading assets CSV - keep numeric fields as strings"""
    return {
        "date": pl.Utf8,
        "personName": pl.Utf8,
        "numberOfShares": pl.Utf8,  # Keep as string to preserve precision
        "sharePrice": pl.Utf8,  # Keep as string to preserve precision
        "exchangeRate": pl.Utf8,  # Keep as string to preserve precision
        "ticker": pl.Utf8,
        "companyName": pl.Utf8,
        "currencyCode": pl.Utf8,
        "exchange": pl.Utf8,
        "interactive": pl.Utf8,
    }


def clean_and_deduplicate(df, dataset_type):
    # Remove records with missing names
    if dataset_type == "billionaires":
        condition = (pl.col("personName").is_null() | (pl.col("personName") == "")) & (
            pl.col("lastName").is_null() | (pl.col("lastName") == "")
        )
    else:  # assets
        condition = pl.col("personName").is_null() | (pl.col("personName") == "")
    df_clean = df.filter(~condition)

    # Create deduplication key
    if dataset_type == "billionaires":
        df_keyed = df_clean.with_columns(
            pl.concat_str(
                [
                    pl.col("date"),
                    pl.col("personName").fill_null(""),
                    pl.col("lastName").fill_null(""),
                ],
                separator="|",
            ).alias("dedup_key")
        )
        sort_col = "finalWorth"
        # Convert finalWorth to decimal for proper sorting
        df_keyed = df_keyed.with_columns(
            pl.when(pl.col(sort_col) == "")
            .then(None)
            .otherwise(pl.col(sort_col))
            .cast(pl.Decimal(precision=18, scale=8))
            .alias(f"{sort_col}_decimal")
        )
        sort_col = f"{sort_col}_decimal"
    else:
        df_keyed = df_clean.with_columns(
            pl.concat_str(
                [
                    pl.col("date"),
                    pl.col("personName").fill_null(""),
                    pl.col("ticker").fill_null(""),
                    pl.col("companyName").fill_null(""),
                    pl.col("currencyCode").fill_null(""),
                    pl.col("exchange").fill_null(""),
                    pl.col("interactive").fill_null(""),
                    pl.col("exchangeRate").fill_null(""),
                ],
                separator="|",
            ).alias("dedup_key")
        )
        sort_col = "numberOfShares"
        # Convert numberOfShares to decimal for proper sorting
        df_keyed = df_keyed.with_columns(
            pl.when(pl.col(sort_col) == "")
            .then(None)
            .otherwise(pl.col(sort_col))
            .cast(pl.Decimal(precision=18, scale=2))
            .alias(f"{sort_col}_decimal")
        )
        sort_col = f"{sort_col}_decimal"

    # Deduplicate (keep highest value record)
    sorted_df = df_keyed.sort(["dedup_key", sort_col], descending=[False, True])
    deduped = sorted_df.unique(subset=["dedup_key"], keep="first")

    # Drop the temporary columns
    columns_to_drop = ["dedup_key"]
    if dataset_type == "billionaires":
        columns_to_drop.append("finalWorth_decimal")
    else:
        columns_to_drop.append("numberOfShares_decimal")

    return deduped.drop(columns_to_drop)


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate billionaire and asset data"
    )
    parser.add_argument(
        "--billionaires", required=True, help="Path to billionaires CSV"
    )
    parser.add_argument("--assets", required=True, help="Path to assets CSV")
    parser.add_argument("--output-dir", default="cleaned_data", help="Output directory")
    args = parser.parse_args()

    # Setup output
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    print("🔍 Reading billionaires data with string schema to preserve precision...")
    # Read with explicit string schema to avoid float conversion
    billionaires = pl.read_csv(
        args.billionaires,
        schema_overrides=get_billionaires_csv_schema(),
        infer_schema_length=0,  # Disable schema inference
    )
    print(f"   ✅ Read {len(billionaires)} billionaire records")

    print("🔍 Reading assets data with string schema to preserve precision...")
    assets = pl.read_csv(
        args.assets,
        schema_overrides=get_assets_csv_schema(),
        infer_schema_length=0,  # Disable schema inference
    )
    print(f"   ✅ Read {len(assets)} asset records")

    print("🧹 Cleaning and deduplicating billionaires...")
    clean_bills = clean_and_deduplicate(billionaires, "billionaires")
    print(f"   ✅ {len(billionaires)} → {len(clean_bills)} records")

    print("🧹 Cleaning and deduplicating assets...")
    clean_assets = clean_and_deduplicate(assets, "assets")
    print(f"   ✅ {len(assets)} → {len(clean_assets)} records")

    # Save results
    print("💾 Saving cleaned data...")
    clean_bills.write_csv(output_dir / "billionaires_clean.csv")
    clean_assets.write_csv(output_dir / "assets_clean.csv")
    print(f"✅ Cleaned data saved to {output_dir}")


if __name__ == "__main__":
    main()
