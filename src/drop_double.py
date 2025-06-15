#!/usr/bin/env python3
import polars as pl
import argparse
from pathlib import Path
import sys


def clean_and_deduplicate(df, dataset_type):
    # Remove records with missing names
    if dataset_type == "billionaires":
        condition = pl.col("personName").is_null() & pl.col("lastName").is_null()
    else:  # assets
        condition = pl.col("personName").is_null()
    df_clean = df.filter(~condition)

    # Create deduplication key
    if dataset_type == "billionaires":
        df_keyed = df_clean.with_columns(
            pl.concat_str(
                [
                    pl.col("date").cast(pl.Utf8),
                    pl.col("personName").fill_null(""),
                    pl.col("lastName").fill_null(""),
                ],
                separator="|",
            ).alias("dedup_key")
        )
        sort_col = "finalWorth"
    else:
        df_keyed = df_clean.with_columns(
            pl.concat_str(
                [
                    pl.col("date").cast(pl.Utf8),
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

    # Deduplicate (keep highest value record)
    sorted_df = df_keyed.sort(["dedup_key", sort_col], descending=[False, True])
    deduped = sorted_df.unique(subset=["dedup_key"], keep="first")
    return deduped.drop("dedup_key")


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

    # Process data
    billionaires = pl.read_csv(args.billionaires)
    assets = pl.read_csv(args.assets)

    clean_bills = clean_and_deduplicate(billionaires, "billionaires")
    clean_assets = clean_and_deduplicate(assets, "assets")

    # Save results
    clean_bills.write_csv(output_dir / "billionaires_clean.csv")
    clean_assets.write_csv(output_dir / "assets_clean.csv")
    print(f"✅ Cleaned data saved to {output_dir}")


if __name__ == "__main__":
    main()
