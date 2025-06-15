#!/usr/bin/env python3
import polars as pl
import argparse
from pathlib import Path
import sys


def get_billionaires_schema():
    return {
        "date": pl.Date,
        "personName": pl.Categorical,
        "lastName": pl.Categorical,
        "birthDate": pl.Date,  # Will handle epoch conversion
        "gender": pl.Categorical,
        "countryOfCitizenship": pl.Categorical,
        "city": pl.Categorical,
        "state": pl.Categorical,
        "source": pl.Categorical,
        "industries": pl.Categorical,
        "finalWorth": pl.Decimal(precision=18, scale=8),
        "estWorthPrev": pl.Decimal(precision=18, scale=8),
        "archivedWorth": pl.Decimal(precision=18, scale=8),
        "privateAssetsWorth": pl.Decimal(precision=18, scale=8),
    }


def get_assets_schema():
    return {
        "date": pl.Date,
        "personName": pl.Categorical,
        "companyName": pl.Categorical,
        "currencyCode": pl.Categorical,
        "currentPrice": pl.Decimal(precision=18, scale=11),
        "exchange": pl.Categorical,
        "exchangeRate": pl.Decimal(precision=18, scale=8),
        "exerciseOptionPrice": pl.Decimal(precision=18, scale=11),
        "interactive": pl.Boolean,
        "numberOfShares": pl.Decimal(precision=18, scale=2),
        "sharePrice": pl.Decimal(precision=18, scale=11),
        "ticker": pl.Categorical,
    }


def get_csv_read_schema(target_schema):
    """Create a schema for reading CSV where all columns are strings to prevent float conversion"""
    csv_schema = {}
    for col_name, _ in target_schema.items():
        csv_schema[col_name] = pl.Utf8
    return csv_schema


def convert_to_parquet(
    csv_file, schema_func, output_file, compression="snappy", sort_columns=None
):
    print(f"📖 Reading {csv_file}...")

    # Get target schema
    target_schema = schema_func()

    # Read CSV with all columns as strings to prevent any automatic float conversion
    csv_read_schema = get_csv_read_schema(target_schema)
    df = pl.read_csv(
        csv_file,
        schema_overrides=csv_read_schema,
        infer_schema_length=0,  # Disable inference completely
    )
    print(f"   Original shape: {df.shape}")
    print(f"   All columns read as strings to preserve precision")

    column_expressions = []

    for col_name, dtype in target_schema.items():
        if col_name not in df.columns:
            # Handle missing columns
            if dtype == pl.Categorical:
                expr = pl.lit(None).cast(pl.Utf8).cast(pl.Categorical)
            else:
                expr = pl.lit(None).cast(dtype)
            column_expressions.append(expr.alias(col_name))
            continue

        # Special handling for epoch birthDate
        if col_name == "birthDate":
            # First check if the value is numeric (epoch timestamp)
            expr = (
                pl.when(pl.col(col_name).str.contains("^[0-9]+$"))
                .then(
                    pl.col(col_name)
                    .cast(pl.Int64)
                    .cast(pl.Datetime(time_unit="ms"))
                    .cast(pl.Date)
                )
                .otherwise(
                    pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                )
                .alias(col_name)
            )
        # Date handling
        elif dtype == pl.Date:
            if col_name == "date":
                # Date is in YYYYMMDD format
                expr = pl.col(col_name).str.strptime(pl.Date, "%Y%m%d", strict=False)
            else:
                expr = pl.col(col_name).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        # Decimal handling - DIRECT conversion from string to Decimal
        elif "Decimal" in str(dtype):
            expr = (
                pl.when(pl.col(col_name) == "")
                .then(None)
                .otherwise(pl.col(col_name))
                .cast(
                    dtype
                )  # Direct cast from string to Decimal, no float intermediate
                .alias(col_name)
            )
        # Boolean handling
        elif dtype == pl.Boolean:
            expr = (
                pl.when(pl.col(col_name).is_in(["True", "true", "1", "TRUE"]))
                .then(True)
                .when(pl.col(col_name).is_in(["False", "false", "0", "FALSE"]))
                .then(False)
                .otherwise(None)
                .alias(col_name)
            )
        # Categorical handling
        elif dtype == pl.Categorical:
            expr = (
                pl.when(pl.col(col_name) == "")
                .then(None)
                .otherwise(pl.col(col_name))
                .cast(pl.Categorical)
                .alias(col_name)
            )
        # Default casting
        else:
            expr = pl.col(col_name).cast(dtype).alias(col_name)

        column_expressions.append(expr)

    print("🔄 Applying schema transformations (string → target types)...")
    df_typed = df.select(column_expressions)
    df_final = df_typed.select(list(target_schema.keys()))

    # Sort data for better compression
    if sort_columns:
        print(f"🔀 Sorting data by {', '.join(sort_columns)}...")
        df_final = df_final.sort(sort_columns)

    print(f"   Final shape: {df_final.shape}")
    print(f"💾 Writing {output_file} with {compression} compression...")
    df_final.write_parquet(output_file, compression=compression)

    # Verify decimal precision was preserved
    print("🔍 Verifying decimal precision...")
    sample_decimals = []
    for col, dtype in target_schema.items():
        if "Decimal" in str(dtype):
            non_null = df_final.filter(pl.col(col).is_not_null()).select(col).limit(5)
            if len(non_null) > 0:
                sample_decimals.append(f"   {col}: {non_null[col].to_list()[0]}")
    if sample_decimals:
        print("   Sample decimal values (first non-null):")
        for sample in sample_decimals[:3]:  # Show first 3 decimal columns
            print(sample)

    return df_final.shape[0]


def main():
    parser = argparse.ArgumentParser(description="Convert CSV to Parquet")
    parser.add_argument("--billionaires", required=True, help="Billionaires CSV path")
    parser.add_argument("--assets", required=True, help="Assets CSV path")
    parser.add_argument("--output-dir", default="parquet_data", help="Output directory")
    parser.add_argument(
        "--compression",
        default="snappy",
        choices=["snappy", "gzip", "lz4", "zstd", "brotli", "uncompressed"],
        help="Compression algorithm (default: snappy)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    print("🚀 Starting CSV to Parquet conversion...")
    print(f"📦 Using {args.compression} compression")
    print(
        "🔒 Reading all numeric values as strings first to preserve decimal precision"
    )

    try:
        # Process billionaires
        print("\n" + "=" * 60)
        print("CONVERTING BILLIONAIRES DATA")
        bill_path = output_dir / "billionaires.parquet"
        bill_rows = convert_to_parquet(
            args.billionaires,
            get_billionaires_schema,
            bill_path,
            args.compression,
            sort_columns=["personName", "date"],
        )

        # Process assets
        print("\n" + "=" * 60)
        print("CONVERTING ASSETS DATA")
        assets_path = output_dir / "assets.parquet"
        asset_rows = convert_to_parquet(
            args.assets,
            get_assets_schema,
            assets_path,
            args.compression,
            sort_columns=["personName", "companyName", "interactive", "date"],
        )

        # Summary
        print("\n" + "=" * 60)
        print(f"✅ Billionaires: {bill_rows:,} rows")
        print(f"✅ Assets: {asset_rows:,} rows")
        print(f"📁 Files saved to: {output_dir.absolute()}")
        print(f"🔒 All decimal values preserved without float conversion")
        return True
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
