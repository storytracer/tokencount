# Tokencount

A Python CLI tool to count tokens in data files using tiktoken with DuckDB for file-format-agnostic data access.

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Basic usage - count tokens in all files with the 'text' field
python tokencount.py /path/to/data text

# Count tokens for a specific model
python tokencount.py /path/to/data text --model gpt-3.5-turbo

# Use a specific number of worker processes
python tokencount.py /path/to/data text --workers 4

# Use a larger batch size for better performance with large files
python tokencount.py /path/to/data text --batch-size 5000
```

## Arguments and Options

- `DATASET_PATH`: Path to the folder containing data files
- `FIELD`: Column/field name containing the text to tokenize
- `--model`: Model name to use for tokenization (default: gpt-4o)
- `--workers`: Number of worker processes (default: number of CPU cores)
- `--batch-size`: Number of rows to process in each batch (default: 1000)

## Supported File Formats

The tool is file-format agnostic and will automatically detect and process any files DuckDB can read:
- JSONL files (plain or gzipped)
- Parquet files
- CSV files
- And more

All files are accessed through DuckDB's glob functionality for uniform handling and improved performance.