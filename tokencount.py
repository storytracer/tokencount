#!/usr/bin/env python3
import os
import tiktoken
import multiprocessing
import duckdb
from tqdm import tqdm
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import click


def count_tokens_in_text(text, encoding):
    """Count the number of tokens in a text string."""
    if not text or not isinstance(text, str):
        return 0
    return len(encoding.encode(text))


def process_chunk(chunk_data, field, encoding):
    """Process a chunk of data and count tokens."""
    total_tokens = 0
    processed_items = 0
    
    for row in chunk_data:
        if row[0]:  # Check if the text exists
            total_tokens += count_tokens_in_text(row[0], encoding)
            processed_items += 1
    
    return total_tokens, processed_items


def process_directory_chunk(chunk_index, total_chunks, dataset_path, field, encoding_name, batch_size=1000):
    """Process a portion of the dataset directory."""
    encoding = tiktoken.get_encoding(encoding_name)
    total_tokens = 0
    processed_items = 0
    
    try:
        with duckdb.connect() as con:
            # Create a wildcard path for all files in the directory
            if isinstance(dataset_path, Path):
                glob_path = str(dataset_path / '*')
            else:
                glob_path = os.path.join(dataset_path, '*')
            
            # First discover all files using glob
            files_query = f"""
                SELECT file FROM glob('{glob_path}')
            """
            files = con.execute(files_query).fetchall()
            
            if not files:
                return f"Chunk {chunk_index+1}/{total_chunks}", 0, 0
            
            # Determine file formats and create appropriate query
            all_data_query = "SELECT * FROM ("
            for i, (file_path,) in enumerate(files):
                file_ext = Path(file_path).suffix.lower()
                
                if file_ext == '.parquet':
                    source = f"parquet_scan('{file_path}')"
                elif file_ext in ('.csv', '.tsv'):
                    source = f"read_csv('{file_path}')"
                elif file_ext in ('.json', '.jsonl'):
                    source = f"read_json('{file_path}')"
                elif file_ext in ('.gz', '.bz2', '.zip', '.xz'):
                    # For compressed files, check the extension before compression
                    base_name = Path(file_path).stem
                    base_ext = Path(base_name).suffix.lower()
                    if base_ext in ('.json', '.jsonl'):
                        source = f"read_json('{file_path}')"
                    elif base_ext in ('.csv', '.tsv'):
                        source = f"read_csv('{file_path}')"
                    else:
                        # Skip files we can't determine format for
                        continue
                else:
                    # Skip files with unknown formats
                    continue
                
                all_data_query += f"(SELECT * FROM {source})"
                if i < len(files) - 1:
                    all_data_query += " UNION ALL "
            
            all_data_query += ")"
            
            # Count total rows to enable chunking
            count_query = f"SELECT COUNT(*) FROM ({all_data_query})"
            total_rows = con.execute(count_query).fetchone()[0]
            
            if total_rows == 0:
                return f"Chunk {chunk_index+1}/{total_chunks}", 0, 0
            
            # Calculate the chunk boundaries
            chunk_size = total_rows // total_chunks
            start_row = chunk_index * chunk_size
            end_row = start_row + chunk_size if chunk_index < total_chunks - 1 else total_rows
            
            # Process in batches to avoid memory issues
            for offset in range(start_row, end_row, batch_size):
                limit = min(batch_size, end_row - offset)
                query = f"""
                    SELECT {field} FROM ({all_data_query})
                    LIMIT {limit} OFFSET {offset}
                """
                
                result_set = con.execute(query).fetchall()
                chunk_tokens, chunk_items = process_chunk(result_set, field, encoding)
                total_tokens += chunk_tokens
                processed_items += chunk_items
                
    except Exception as e:
        click.echo(f"Error processing directory chunk {chunk_index+1}/{total_chunks}: {e}", err=True)
    
    return f"Chunk {chunk_index+1}/{total_chunks}", total_tokens, processed_items


@click.command()
@click.argument('dataset_path', type=click.Path(exists=True))
@click.argument('field', type=str)
@click.option('--model', default='gpt-4o', help='Model name to use for tokenization')
@click.option('--workers', default=multiprocessing.cpu_count(), help='Number of worker processes')
@click.option('--batch-size', default=1000, help='Batch size for processing rows')
def main(dataset_path, field, model, workers, batch_size):
    """Count tokens in files using tiktoken.
    
    DATASET_PATH is the path to the folder containing data files.
    FIELD is the column/field name containing the text to tokenize.
    
    The tool is file-format agnostic and will automatically detect and process
    any files DuckDB can read (jsonl, jsonl.gz, parquet, csv, etc.)
    """
    try:
        encoding_name = tiktoken.encoding_for_model(model).name
    except KeyError:
        click.echo(f"Model {model} not found. Using 'o200k_base' encoding instead.")
        encoding_name = "o200k_base"
    
    click.echo(f"Using encoding: {encoding_name}")
    
    dataset_path = Path(dataset_path)
    click.echo(f"Processing dataset: {dataset_path}")
    click.echo(f"Field to tokenize: {field}")
    click.echo(f"Using {workers} worker processes")
    
    # Set up chunks and multiprocessing
    chunks = workers
    process_func = partial(
        process_directory_chunk, 
        total_chunks=chunks,
        dataset_path=dataset_path,
        field=field,
        encoding_name=encoding_name,
        batch_size=batch_size
    )
    
    # Process the chunks in parallel
    with ProcessPoolExecutor(max_workers=workers) as executor:
        results = list(tqdm(executor.map(process_func, range(chunks)), total=chunks))
    
    # Aggregate results
    total_tokens = 0
    total_processed = 0
    chunk_results = []
    
    for chunk_name, tokens, processed in results:
        total_tokens += tokens
        total_processed += processed
        chunk_results.append({
            'chunk': chunk_name,
            'tokens': tokens,
            'items_processed': processed,
        })
    
    # Sort results by token count (descending)
    chunk_results.sort(key=lambda x: x['tokens'], reverse=True)
    
    # Print summary
    click.echo(f"\nTotal tokens: {total_tokens}")
    click.echo(f"Total items processed: {total_processed}")
    if total_processed > 0:
        click.echo(f"Average tokens per item: {total_tokens / total_processed:.2f}")
    else:
        click.echo("Average tokens per item: 0.00")


if __name__ == '__main__':
    main()