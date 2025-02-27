# Tokencount

A Python CLI tool to count tokens in data files using tiktoken for efficient processing of various file formats.

## Installation

### Option 1: Install from source (development mode)

```bash
# Clone the repository
git clone https://github.com/eleuther/tokencount.git
cd tokencount

# Create a virtual environment (optional but recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .
```

### Option 2: Install directly from requirements

```bash
pip install -r requirements.txt
```

### Option 3: Install as a package (after cloning)

```bash
pip install .
```

## Usage

```bash
# After installation as a package:
tokencount /path/to/data text

# Or if running from source:
python tokencount.py /path/to/data text

# Count tokens for a specific model
tokencount /path/to/data text --model gpt-3.5-turbo

# Use a larger batch size for better performance with large files
tokencount /path/to/data text --batch-size 5000
```

## Arguments and Options

- `DATASET_PATH`: Path to the folder containing data files
- `FIELD`: Column/field name containing the text to tokenize
- `--model`: Model name to use for tokenization (default: gpt-4o)
- `--batch-size`: Number of rows to process in each batch (default: 1000)

## Supported File Formats

The tool uses Hugging Face's datasets library and supports various file formats:
- JSONL files (plain or gzipped)
- Parquet files
- CSV files
- And more

Datasets are loaded directly from the path without requiring any symlinks or special configuration.