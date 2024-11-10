# 🗃️ SQLParser 🗃️ 
by @shoewind1997

Extract sensitive data from SQL value lines into CSV

## Usage

`usage: sqlparser.py [-h] [-c CONFIG] -i INPUT [-o OUTPUT] [--chunk-size CHUNK_SIZE]`

## Options
```
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to config file (default: config.json)
  -i INPUT, --input INPUT
                        Path to input SQL file
  -o OUTPUT, --output OUTPUT
                        Path to output file (default: sensitive_data.txt)
  --chunk-size CHUNK_SIZE
                        Number of records to process before writing (default: 1000)
```

## Difference between SQLMiner and SQLParser:
 - SQLMiner is meant to pull out data from well-structured SQL dumps, complete with `CREATE TABLE` statements and/or `INSERT INTO` statements that have column headings defined, allowing for schema extraction
 - SQLParser is geared towards files that have already been reduces and trimmed out, leaving only the statements that contain the data, having to infer what information is useful and ignoring the rest (e.g. extra characters like `(`, `)`, quotes, and so on)