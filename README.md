# Forbes Billionaire Data Pipeline & Parquet Optimization

A data engineering pipeline that downloads historical Forbes billionaire data from the Wayback Machine and optimizes it for analytical workloads through advanced Parquet compression techniques.

## 🎯 Project Goal

Transform large-scale financial datasets (12.5M+ records) into highly compressed, query-optimized Parquet files while preserving decimal precision for financial calculations.

## 📊 Dataset

- **Billionaires**: 4.8M records tracking wealth over time (14 fields)
- **Assets**: 7.7M records of individual holdings (12 fields)
- **Timespan**: Historical Forbes data from 2020+
- **Precision**: Financial values preserved with full decimal accuracy

## 🚀 Key Achievement

**79% size reduction**: 346MB → 72MB through intelligent compression optimization

| Stage | Technique | Size | Reduction |
|-------|-----------|------|-----------|
| Original | CSV → Parquet (Snappy) | 346MB | - |
| Sorted | Data organization | 144MB | 58% |
| **Final** | **Zstd + Multi-column sorting** | **72MB** | **79%** |

## 🛠️ Pipeline Overview

```
Forbes API → Wayback Machine → JSON → CSV → Cleaned Data → Optimized Parquet
```

1. **Data Acquisition**: Download historical Forbes API snapshots
2. **Conversion**: JSON → CSV with preserved precision
3. **Cleaning**: Deduplication and data validation
4. **Optimization**: Advanced sorting + Zstd compression

## 📦 Usage

```bash
# 1. Download historical data
python src/get_data.py --start-date 2020-01-01 --output-dir json_files

# 2. Convert to CSV
python src/convert_csv.py json_files --output-prefix raw_data

# 3. Clean and deduplicate
python src/drop_double.py --billionaires raw_data_billionaires.csv --assets raw_data_assets.csv

# 4. Create optimized Parquet files
python src/convert_parquet.py \
  --billionaires cleaned_data/billionaires_clean.csv \
  --assets cleaned_data/assets_clean.csv \
  --compression zstd
```

## 🔧 Optimization Techniques

- **Smart Sorting**: Multi-level organization (person → company → type → date)
- **Zstd Compression**: Best balance of compression ratio and speed
- **Type Optimization**: Proper decimal handling without float conversion
- **Schema Design**: Categorical encoding for repeated strings

## 📈 Performance Benefits

- **Storage**: 79% reduction in disk space
- **I/O**: Faster data loading for analytics
- **Precision**: Full financial accuracy maintained
- **Queries**: Data physically organized for efficient person/company analysis

## 🔍 Technical Highlights

- Handles epoch timestamps and multiple date formats
- Preserves decimal precision throughout the pipeline
- Graceful handling of schema inconsistencies
- Memory-efficient processing of large datasets

---

*Perfect for financial analysis, wealth tracking studies, and an illustration of advanced Parquet optimization techniques.*
