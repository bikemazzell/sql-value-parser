import re
import csv
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Callable
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil
import mmap
from functools import lru_cache
import json
import argparse
from contextlib import ExitStack

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_config(config_path: Path) -> tuple[Dict[str, re.Pattern], set[bytes]]:
    """Load and compile regex patterns from config file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Config file '{config_path}' not found")
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate and compile regex patterns
        patterns = {}
        for name, pattern in config['patterns'].items():
            try:
                patterns[name] = re.compile(pattern.encode('utf-8'))
            except re.error as e:
                raise ValueError(f"Invalid regex pattern '{name}': {str(e)}")
        
        # Convert ignore values to bytes
        ignore_values = {
            value.encode('utf-8')
            for value in config['ignore_values']
        }
        
        return patterns, ignore_values
    
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {config_path}: {str(e)}")
    except KeyError as e:
        raise ValueError(f"Missing required key in {config_path}: {str(e)}")
    except Exception as e:
        raise Exception(f"Error loading {config_path}: {str(e)}")

@lru_cache(maxsize=1024)
def validate_date(date_str: str) -> bool:
    """Validate date string with caching for better performance."""
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def parse_sql_line(line: bytes) -> List[str]:
    """Process a single line of SQL data using bytes."""
    found_items = []
    seen = set()
    
    for pattern_type, pattern in PATTERNS.items():
        matches = pattern.findall(line)
        for match in matches:
            if isinstance(match, tuple):
                match = match[0]
            
            # Clean up quotes but keep the content
            clean_match = match.strip(b"'\"")
            
            if len(clean_match) < 3 or clean_match.lower() in IGNORE_VALUES:
                continue
            
            if clean_match.isdigit() and len(clean_match) < 4:
                continue
                
            if pattern_type == 'date':
                try:
                    date_str = clean_match.split()[0].decode('utf-8', errors='ignore')
                    if not validate_date(date_str):
                        continue
                    match = date_str.encode('utf-8')
                except (ValueError, UnicodeDecodeError):
                    continue
            
            # Handle JSON strings - preserve the content but clean up escaping
            if clean_match.startswith(b'{') and clean_match.endswith(b'}'):
                try:
                    json_str = clean_match.decode('utf-8', errors='ignore')
                    # Remove escaped quotes
                    json_str = json_str.replace('\\"', '"')
                    # Parse to validate and include as is
                    json.loads(json_str)
                    match = json_str.encode('utf-8')
                except (json.JSONDecodeError, Exception):
                    continue
            else:
                match = clean_match

            if match not in seen:
                seen.add(match)
                found_items.append(match)
    
    return [x.decode('utf-8', errors='ignore') for x in found_items]

def process_chunk(chunk: bytes, chunk_number: int) -> tuple[List[List[str]], int]:
    """Process a chunk of bytes and return results with chunk number for ordering."""
    results = []
    for line in chunk.splitlines():
        if line := line.strip():
            if items := parse_sql_line(line):
                # Remove quotes from each item
                cleaned_items = [item.replace('"', '').replace("'", "") for item in items]
                results.append(cleaned_items)
    return results, chunk_number

def get_chunk_size(user_chunk_size: Optional[int] = None) -> int:
    """Calculate or set chunk size based on user input or available memory."""
    if user_chunk_size:
        return user_chunk_size

    cpu_count = psutil.cpu_count()
    available_memory = psutil.virtual_memory().available
    return max(min(available_memory // (cpu_count * 4), 1024 * 1024 * 10), 1024 * 1024)

def process_file(
    input_file: str, 
    output_file: str, 
    chunk_size: int,
    batch_size: int = 10000,
    progress_callback: Optional[Callable[[float, int, int], None]] = None
):
    """
    Process the input SQL file and write results to CSV.
    
    Args:
        input_file: Path to input SQL file
        output_file: Path to output CSV file
        chunk_size: Size of chunks to process
        batch_size: Number of results to buffer before writing
        progress_callback: Optional callback for progress updates
    """
    try:
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        results_buffer = []
        
        with ExitStack() as stack:
            infile = stack.enter_context(open(input_file, 'rb'))
            outfile = stack.enter_context(open(output_file, 'w', newline='', encoding='utf-8'))
            executor = stack.enter_context(ProcessPoolExecutor(max_workers=psutil.cpu_count()))
            mm = stack.enter_context(mmap.mmap(infile.fileno(), 0, access=mmap.ACCESS_READ))
            
            file_size = mm.size()
            csv_writer = csv.writer(outfile)
            futures = {}
            current_pos = 0
            chunk_number = 0
            
            while current_pos < file_size:
                chunk = mm[current_pos:current_pos + chunk_size]
                if current_pos + chunk_size < file_size:
                    next_newline = chunk.rfind(b'\n') + 1
                    if next_newline > 0:
                        chunk = chunk[:next_newline]
                
                futures[executor.submit(process_chunk, chunk, chunk_number)] = chunk_number
                current_pos += len(chunk)
                chunk_number += 1
                
                for future in as_completed(futures):
                    results, _ = future.result()
                    results_buffer.extend(results)
                    del futures[future]
                    
                    if len(results_buffer) >= batch_size:
                        for items in results_buffer:
                            csv_writer.writerow(items)
                        results_buffer.clear()
                    
                    progress = (current_pos / file_size) * 100
                    if progress_callback:
                        progress_callback(progress, current_pos, file_size)
                    else:
                        logging.info(f"Progress: {progress:.2f}% ({current_pos}/{file_size} bytes)")
            
            # Write remaining results
            for items in results_buffer:
                csv_writer.writerow(items)
        
        logging.info(f"Processing complete. Output saved to: {output_file}")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise

def cleanup_temp_files(output_file: str):
    """Clean up any temporary files created during processing."""
    try:
        temp_path = Path(output_file + '.tmp')
        if temp_path.exists():
            temp_path.unlink()
    except Exception as e:
        logging.warning(f"Failed to cleanup temporary files: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description='🗃️ SQLParser 🗃️ by @shoewind1997 : Process SQL file and extract patterns')
    parser.add_argument('-c', '--config', type=Path, default='config.json',
                      help='Path to config file (default: config.json)')
    parser.add_argument('-i', '--input', required=True,
                      help='Path to input SQL file')
    parser.add_argument('-o', '--output',
                      help='Path to output file (default: input_filename_out.csv)')
    parser.add_argument('--chunk-size', type=int, default=1000,
                      help='Number of records to process before writing (default: 1000)')
    parser.add_argument('--batch-size', type=int, default=10000,
                      help='Number of results to buffer before writing (default: 10000)')
    
    args = parser.parse_args()
    
    # Load patterns and ignore values from config file
    try:
        global PATTERNS, IGNORE_VALUES
        PATTERNS, IGNORE_VALUES = load_config(args.config)
    except Exception as e:
        logging.error(str(e))
        sys.exit(1)
    
    if not Path(args.input).exists():
        logging.error(f"Error: Input file '{args.input}' does not exist.")
        sys.exit(1)
    
    # If output file is not specified, generate default name
    if args.output is None:
        input_path = Path(args.input)
        output_file = str(input_path.parent / f"{input_path.stem}_out.csv")
    else:
        output_file = args.output
    
    try:
        process_file(args.input, output_file, args.chunk_size, args.batch_size)
    except KeyboardInterrupt:
        logging.info("Processing interrupted by user")
        cleanup_temp_files(output_file)
        sys.exit(0)
    except Exception as e:
        logging.error(f"Processing failed: {str(e)}")
        cleanup_temp_files(output_file)
        sys.exit(1)

if __name__ == "__main__":
    main()