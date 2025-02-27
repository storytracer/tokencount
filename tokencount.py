#!/usr/bin/env python3
import tiktoken
from datasets import load_dataset
from tqdm import tqdm
import click
import os
from pathlib import Path


def count_tokens_in_text(text, encoding):
    """Count the number of tokens in a text string."""
    if not text or not isinstance(text, str):
        return 0
    return len(encoding.encode(text))


@click.command()
@click.argument('dataset_path', type=click.Path(exists=True))
@click.argument('field', type=str)
@click.option('--model', default='gpt-4o', help='Model name to use for tokenization')
@click.option('--batch-size', default=1000, help='Batch size for processing rows')
def main(dataset_path, field, model, batch_size):
    """Count tokens in files using tiktoken.
    
    DATASET_PATH is the path to the folder containing data files.
    FIELD is the column/field name containing the text to tokenize.
    
    The tool uses HuggingFace datasets to load data files and is
    format-agnostic (supports jsonl, json, parquet, csv, etc.)
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
        encoding_name = encoding.name
    except KeyError:
        click.echo(f"Model {model} not found. Using 'o200k_base' encoding instead.")
        encoding_name = "o200k_base"
        encoding = tiktoken.get_encoding(encoding_name)
    
    click.echo(f"Using encoding: {encoding_name}")
    
    dataset_path = Path(dataset_path).resolve()
    click.echo(f"Processing dataset: {dataset_path}")
    click.echo(f"Field to tokenize: {field}")
    
    click.echo("Loading dataset...")
    
    try:
        # Load the dataset directly from the path
        dataset = load_dataset(
            str(dataset_path),
            streaming=True
        )
        
        # Handle different dataset structures
        if isinstance(dataset, dict) and 'train' in dataset:
            dataset = dataset['train']
        
        click.echo("Dataset loaded successfully")
        
        # Initialize counters
        total_tokens = 0
        total_processed = 0
        
        # Process the dataset (streaming compatible)
        progress_bar = tqdm(dataset, desc="Tokenizing", unit="tokens")
        for item in dataset:
            if field in item and item[field]:
                text = item[field]
                tokens_count = count_tokens_in_text(text, encoding)
                total_tokens += tokens_count
                total_processed += 1
                
                # Update progress bar with token count
                progress_bar.update(tokens_count)
        
        # Print summary
        click.echo(f"\nTotal tokens: {total_tokens}")
        click.echo(f"Total items processed: {total_processed}")
        if total_processed > 0:
            click.echo(f"Average tokens per item: {total_tokens / total_processed:.2f}")
        else:
            click.echo("Average tokens per item: 0.00")
            
    except Exception as e:
        click.echo(f"Error processing dataset: {e}", err=True)


def cli():
    """Entry point for CLI."""
    main()


if __name__ == '__main__':
    main()