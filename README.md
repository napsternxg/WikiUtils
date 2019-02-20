# WikiUtils
A set of utility scripts to process Wikipedia related data


## parse\_mysqldump

A script for parsing wikipedia mysqldump `sql.gz` files. Can be extended to parse arbitraty mysqldump files. 

```
usage: parse_mysqldump.py [-h] [--column-indexes COLUMN_INDEXES]
                          filename filetype outputfile

positional arguments:
  filename              name of the wikipedia sql.gz file.
  filetype              following filetypes are supported: [categorylinks,
                        pagelinks, redirect, category, page_props, page]
  outputfile            name of the output file

optional arguments:
  -h, --help            show this help message and exit
  --column-indexes COLUMN_INDEXES, -c COLUMN_INDEXES
                        column indexes to use in output file
```


## Inspecting dump files in BASH

Run the following command to parse the dump files in bash. The first and last lines will have some non column information. 

```
zcat enwiki-20170920-categorylinks.sql.gz | grep $'^INSERT INTO ' | sed 's/),(/\n/g' | less -N
```
