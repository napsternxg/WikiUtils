# coding: utf-8

import re
import gzip
import sys
from tqdm import tqdm 
from collections import namedtuple

FILEPROPS=namedtuple("Fileprops", "parser num_fields column_indexes")

CATEGORYLINKS_PARSER=re.compile(r'\((?P<row0>[0-9]+?),(?P<row1>\'.*?\'?),(?P<row2>\'.*?\'?),(?P<row3>\'[0-9\ \-:]+\'?),(?P<row4>\'\'?),(?P<row5>\'.*?\'?),(?P<row6>\'.*?\'?)\)')
REDIRECT_PARSER=re.compile(r'\((?P<row0>[0-9]+?),(?P<row1>[0-9]+?),(?P<row2>\'.*?\'?),(?P<row3>\'.*?\'?),(?P<row4>\'.*?\'?)\)')
CATEGORY_PARSER=re.compile(r'\((?P<row0>[0-9]+?),(?P<row1>\'.*?\'?),(?P<row2>[0-9]+?),(?P<row3>[0-9]+?),(?P<row4>[0-9]+?)\)')
PAGE_PROPS_PARSER=re.compile(r'\(([0-9]+),(\'.*?\'),(\'.*?\'),(\'[0-9\ \-:]+\'),(\'\'),(\'.*?\'),(\'.*?\')\)')
PAGE_PARSER=re.compile((r'\((?P<row0>[0-9]+?),(?P<row1>[0-9]+?),(?P<row2>\'.*?\'?),(?P<row3>\'.*?\'?),(?P<row4>[0-9]+?),(?P<row5>[0-9]+?),(?P<row6>[0-9]?),'
    r'(?P<row7>[0-9\.]+?),(?P<row8>\'.*?\'?),(?P<row9>\'.*?\'?),(?P<row10>[0-9]+?),(?P<row11>[0-9]+?),(?P<row12>(?P<row12val>\'.*?\'?)|(?P<row12null>NULL)),(?P<row13>(?P<row13val>\'.*?\'?)|(?P<row13null>NULL))\)'))

PAGE_PARSER=re.compile((r'(?P<row0>[0-9]+?),(?P<row1>[0-9]+?),(?P<row2>\'.*?\'?),(?P<row3>\'.*?\'?),(?P<row4>[0-9]+?),(?P<row5>[0-9]+?),(?P<row6>[0-9]?),'
    r'(?P<row7>[0-9\.]+?),(?P<row8>\'.*?\'?),(?P<row9>\'.*?\'?),(?P<row10>[0-9]+?),(?P<row11>[0-9]+?),(?P<row12>(?P<row12val>\'.*?\'?)|(?P<row12null>NULL)),(?P<row13>(?P<row13val>\'.*?\'?)|(?P<row13null>NULL))'))

"""
`page_id` int(8) unsigned NOT NULL AUTO_INCREMENT,
`page_namespace` int(11) NOT NULL DEFAULT '0',
`page_title` varbinary(255) NOT NULL DEFAULT '',
`page_restrictions` tinyblob NOT NULL,
`page_counter` bigint(20) unsigned NOT NULL DEFAULT '0',
`page_is_redirect` tinyint(1) unsigned NOT NULL DEFAULT '0',
`page_is_new` tinyint(1) unsigned NOT NULL DEFAULT '0',
`page_random` double unsigned NOT NULL DEFAULT '0',
`page_touched` varbinary(14) NOT NULL DEFAULT '',
`page_links_updated` varbinary(14) DEFAULT NULL,
`page_latest` int(8) unsigned NOT NULL DEFAULT '0',
`page_len` int(8) unsigned NOT NULL DEFAULT '0',
`page_content_model` varbinary(32) DEFAULT NULL,
`page_lang` varbinary(35) DEFAULT NULL,

"""


FILETYPE_PROPS=dict(
        categorylinks=FILEPROPS(CATEGORYLINKS_PARSER, 7, (0, 1, 6)),
        redirect=FILEPROPS(REDIRECT_PARSER, 5, (0, 1, 2)),
        category=FILEPROPS(CATEGORY_PARSER, 5, (0, 1, 2, 3, 4)),
        page_props=FILEPROPS(PAGE_PROPS_PARSER, 7, (0, 1)),
        page=FILEPROPS(PAGE_PARSER, 14, (0, 1, 2, 5, 11, 12, 13)),
        )

#VALUE_PARSER=re.compile(r'\(([0-9]+),(\'.*?\'),(\'.*?\'),(\'[0-9\ \-:]+\'),(\'\'),(\'.*?\'),(\'.*?\')\)')

def process_file(fp, fp_out, filetype, column_indexes=None):
    if filetype not in FILETYPE_PROPS:
        raise Exception("Invalid filetype: {}".format(filetype))
    parser, num_fields, ci = FILETYPE_PROPS[filetype]
    print("Parser: {}\nnum_fields: {}\nci: {}".format(parser, num_fields, ci))
    valid_row_keys = set(["row{}".format(i) for i in range(num_fields)])
    if column_indexes is None:
        column_indexes = ci
    def parse_match(match):
        row = match.groupdict()
        return tuple(row["row{}".format(i)] for i in column_indexes)
    with tqdm() as pbar:
        for i, line in enumerate(fp):
            if line.startswith('INSERT INTO `{}` VALUES '.format(filetype)):
                start, partition, values = line.partition(' VALUES ')
                # replace unicode dash with ascii dash
                values = values.strip()[1:-1].split("),(")
                for value in values:
                    value = value.replace("\\xe2\\x80\\x93", "-")
                    for i, match in enumerate(parser.finditer(value)):
                        try:
                            pbar.update(1)
                            row = parse_match(match)
                            print("\t".join(row), file=fp_out)
                        except Exception as e:
                            print("Line: {}, Exception: {}".format(i, e), file=sys.stderr)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("filename",
            help="name of the wikipedia sql.gz file.")
    parser.add_argument("filetype",
            help="following filetypes are supported:\n[{}]".format(
                ",".join(FILETYPE_PROPS.keys())
                ))
    parser.add_argument("outputfile", 
            help="name of the output file")
    parser.add_argument("--column-indexes", "-c", default=None,
            help="column indexes to use in output file")
    args = parser.parse_args()

    print(args)
    with gzip.open(args.filename, 'rt', encoding='ascii', errors='backslashreplace') as fp, open(args.outputfile, 'wt', encoding='utf-8') as fp_out:
        process_file(fp, fp_out, args.filetype, column_indexes=args.column_indexes)
    
    #with gzip.open("enwiki-20170920-categorylinks.sql.gz", 'rt') as fp, open("categorylinks.txt", "wt") as fp_out:

if __name__ == "__main__":
    main()
