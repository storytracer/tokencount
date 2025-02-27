# CLAUDE.md - Coding Guidelines for Tokencount

## Commands
- Install: `pip install -r requirements.txt`
- Run: `python tokencount.py /path/to/data field [options]`
- Create venv: `python -m venv venv && source venv/bin/activate`
- Run with profiling: `python -m cProfile -o profile.prof tokencount.py /path/to/data field`

## Code Style
- Follow PEP 8 conventions
- Use type hints for function parameters and return values
- Use docstrings for all functions and classes
- Maximum line length: 100 characters
- Import order: standard library, third-party libraries, local modules
- Variable naming: snake_case for variables and functions
- Error handling: Use try/except blocks with specific exceptions
- Prefer f-strings for string formatting

## Architecture
- Use Click for CLI interfaces
- Use DuckDB for file-format-agnostic data access
- Utilize multiprocessing with chunk-based processing
- Process data in batches to manage memory usage
- Use DuckDB's glob functionality for efficient file access
- Ensure resilient error handling to prevent crashes