#!/usr/bin/env python3
import json
import argparse
from pathlib import Path
from collections import defaultdict


def analyze_decimal_precision(value):
    """Simple precision analysis: split on dot and count digits"""
    if value is None:
        return 0, 0
    
    # Convert to string and clean
    str_val = str(value).strip()
    
    # Split on decimal point
    if '.' in str_val:
        before_dot, after_dot = str_val.split('.', 1)
        # Remove trailing zeros from after decimal
        after_dot = after_dot.rstrip('0')
        digits_before = len(before_dot.lstrip('-'))  # Remove minus sign
        digits_after = len(after_dot)
    else:
        digits_before = len(str_val.lstrip('-'))  # Remove minus sign
        digits_after = 0
    
    return digits_before, digits_after


def analyze_json_files(json_folder):
    """Analyze specific numerical fields in JSON files"""
    
    # Define the fields we know are numerical
    billionaire_fields = [
        'finalWorth', 'estWorthPrev', 'archivedWorth', 'privateAssetsWorth'
    ]
    
    asset_fields = [
        'numberOfShares', 'sharePrice', 'exchangeRate', 'exerciseOptionPrice', 'currentPrice'
    ]
    
    all_fields = billionaire_fields + asset_fields
    
    # Track max precision for each field
    precision_stats = {field: {'before': 0, 'after': 0, 'max_left_example': '', 'max_right_example': ''} for field in all_fields}
    
    json_files = sorted(Path(json_folder).glob("*.json"))
    if not json_files:
        print(f"No JSON files found in {json_folder}")
        return None
    
    print(f"Analyzing {len(json_files)} JSON files...")
    
    for i, json_file in enumerate(json_files, 1):
        if i % 10 == 0:
            print(f"Processed {i}/{len(json_files)} files...")
        
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Get records from different possible structures
            records = (
                data.get("personList", {}).get("personsLists") or
                data.get("personList") or 
                data.get("data", []) or
                [data]  # Single record
            )
            
            for record in records:
                # Analyze billionaire-level fields
                for field in billionaire_fields:
                    if field in record:
                        before, after = analyze_decimal_precision(record[field])
                        
                        # Update max left digits and example
                        if before > precision_stats[field]['before']:
                            precision_stats[field]['before'] = before
                            precision_stats[field]['max_left_example'] = str(record[field])
                        
                        # Update max right digits and example
                        if after > precision_stats[field]['after']:
                            precision_stats[field]['after'] = after
                            precision_stats[field]['max_right_example'] = str(record[field])
                
                # Analyze asset-level fields
                for asset in record.get('financialAssets', []):
                    for field in asset_fields:
                        if field in asset:
                            before, after = analyze_decimal_precision(asset[field])
                            
                            # Update max left digits and example
                            if before > precision_stats[field]['before']:
                                precision_stats[field]['before'] = before
                                precision_stats[field]['max_left_example'] = str(asset[field])
                            
                            # Update max right digits and example
                            if after > precision_stats[field]['after']:
                                precision_stats[field]['after'] = after
                                precision_stats[field]['max_right_example'] = str(asset[field])
        
        except Exception as e:
            print(f"Error processing {json_file}: {e}")
            continue
    
    return precision_stats


def print_results(stats):
    """Print analysis results"""
    print("\n" + "="*80)
    print("NUMERICAL PRECISION ANALYSIS")
    print("="*80)
    
    billionaire_fields = ['finalWorth', 'estWorthPrev', 'archivedWorth', 'privateAssetsWorth']
    asset_fields = ['numberOfShares', 'sharePrice', 'exchangeRate', 'exerciseOptionPrice', 'currentPrice']
    
    print("\n📊 BILLIONAIRE FIELDS:")
    print(f"{'Field':<20} | {'Left':<2} | {'Right':<2} | {'Total':<2} | {'Max Left Example':<15} | {'Max Right Example'}")
    print("-" * 80)
    for field in billionaire_fields:
        if stats[field]['before'] > 0 or stats[field]['after'] > 0:
            before = stats[field]['before']
            after = stats[field]['after']
            total = before + after
            max_left = stats[field]['max_left_example']
            max_right = stats[field]['max_right_example']
            print(f"{field:<20} | {before:>2d} | {after:>2d} | {total:>2d} | {max_left:<15} | {max_right}")
    
    print("\n💰 ASSET FIELDS:")
    print(f"{'Field':<20} | {'Left':<2} | {'Right':<2} | {'Total':<2} | {'Max Left Example':<15} | {'Max Right Example'}")
    print("-" * 80)
    for field in asset_fields:
        if stats[field]['before'] > 0 or stats[field]['after'] > 0:
            before = stats[field]['before']
            after = stats[field]['after']
            total = before + after
            max_left = stats[field]['max_left_example']
            max_right = stats[field]['max_right_example']
            print(f"{field:<20} | {before:>2d} | {after:>2d} | {total:>2d} | {max_left:<15} | {max_right}")


def generate_schemas(stats):
    """Generate optimized schemas"""
    print("\n" + "="*60)
    print("GENERATED SCHEMAS")
    print("="*60)
    
    billionaire_fields = ['finalWorth', 'estWorthPrev', 'archivedWorth', 'privateAssetsWorth']
    asset_fields = ['numberOfShares', 'sharePrice', 'exchangeRate', 'exerciseOptionPrice', 'currentPrice']
    
    print("\ndef get_billionaires_schema():")
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
        if stats[field]['before'] > 0 or stats[field]['after'] > 0:
            total = stats[field]['before'] + stats[field]['after']
            scale = stats[field]['after']
            print(f'        "{field}": pl.Decimal(precision={total}, scale={scale}),')
    
    print("    }")
    
    print("\ndef get_assets_schema():")
    print("    return {")
    print('        "date": pl.Date,')
    print('        "personName": pl.Utf8,')
    print('        "ticker": pl.Utf8,')
    print('        "companyName": pl.Utf8,')
    print('        "currencyCode": pl.Utf8,')
    print('        "exchange": pl.Utf8,')
    print('        "interactive": pl.Boolean,')
    
    for field in asset_fields:
        if stats[field]['before'] > 0 or stats[field]['after'] > 0:
            total = stats[field]['before'] + stats[field]['after']
            scale = stats[field]['after']
            print(f'        "{field}": pl.Decimal(precision={total}, scale={scale}),')
    
    print("    }")


def main():
    parser = argparse.ArgumentParser(description="Analyze numerical precision in Forbes JSON files")
    parser.add_argument("json_folder", help="Folder containing JSON files")
    args = parser.parse_args()
    
    # Analyze files
    stats = analyze_json_files(args.json_folder)
    
    if not stats:
        return False
    
    # Print results
    print_results(stats)
    
    # Generate schemas
    generate_schemas(stats)
    
    return True


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
