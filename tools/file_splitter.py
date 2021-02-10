#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import sys
import getopt
import stat

HEX_STR = "0123456789ABCDEF"
 
def usage():
    print ('''
        Usage: Divide the file into #<total_chunks> parts and output the <chunk_idx>th chunk.
        This script is mainly used for Doris to import large local files through Stream Load.
        This script will divide the data into complete rows according to the given line delimiter,
        so it will not cause data loss

         file_splitter <-f file_from> <-c total_chunks> <-s chunk_idx> <-d line_delimiter> [-h help]

        Eg:
          file_splitter -f large.txt -c 3 -s 1 -d '\\n' | curl --location-trusted -u root: -T - http://127.0.0.1:8030/api/db1/tbl1/_stream_load
          file_splitter -f large.txt -c 3 -s 2 -d '\\n' | curl --location-trusted -u root: -T - http://127.0.0.1:8030/api/db1/tbl1/_stream_load
          file_splitter -f large.txt -c 3 -s 3 -d '\\n' | curl --location-trusted -u root: -T - http://127.0.0.1:8030/api/db1/tbl1/_stream_load

        line delimiter support "Escape character", "Multi bytes" and "Hex".
        Eg:
            -d '\\n'
            -d '\\r\\n'
            -d '###'
            -d '\\x070a'
      ''')

def split_output(file_name, chunk_num, chunk_idx, line_delimiter):
    rfd = 0
    chunk_num = int(chunk_num)
    chunk_idx = int(chunk_idx)

    if (chunk_idx > chunk_num):
        sys.stderr.write("error: chunk index is larger then chunk number: %s vs %s\n" % (chunk_idx, chunk_num))
        return -1

    if chunk_idx < 0 or chunk_num < 0:
        sys.stderr.write("invalid  chunk index or chunk number: %s, %s\n" % (chunk_idx, chunk_num))
        return -1

    try:
        rfd = os.open(file_name, os.O_RDONLY)
        f_stat = os.stat(file_name)
 
        # st_size will be 0 if file_name is a device.
        if not stat.S_ISREG(f_stat.st_mode):
            sys.stderr.write("error: invalid file: %s\n" % file_name)
            return -1

        file_size = f_stat.st_size
        if file_size == 0:
            sys.stderr.write("error: empty file: %s\n" % file_name)
            return -1

        #if file_size < 10 * 1024 * 1024:
            # not support small file
            #sys.stderr.write("error: file size is too small: %s\n" % file_size)
            #return -1
        
        if file_size < chunk_num:
            sys.stderr.write("error: chunk number is larger then file size: %s vs %s\n" % (chunk_num, file_size))
            return -1
            
        size_per_chunk = int(max(file_size / chunk_num, 1))
        # start and end offset is range as [start, end)
        start_offset = size_per_chunk * (chunk_idx - 1)
        end_offset = size_per_chunk * chunk_idx
        if chunk_idx == chunk_num:
            # last chunk, read to the end of file
            end_offset = file_size

        # We split file by size, so the content starting from start_offset may be
        # the middle part of a row of data.
        # We need to go forward to find the beginning of the next line to ensure
        # that no data line is lost.
        # At the same time, we have to find the end of the last line.

        line_delimiter_bytes = bytes(line_delimiter, encoding='utf-8')
        delimiter_len = len(line_delimiter_bytes)
        # 1. Handle start offset
        if start_offset != 0:
            # Not the first chunk, seek forward to find the beginning of next row.
            # start_offset may point to the middle of the line delimiter,
            # so the search will start before the length of a line delimiter.
            start_offset = max(start_offset - delimiter_len, 0)

            found_next_row = False
            max_read_length = end_offset - start_offset;
            os.lseek(rfd, max(start_offset, 0), 0)
            while (not found_next_row) and max_read_length > 0:
                rlen = min(4096, max_read_length)
                buf = os.read(rfd, rlen)
                idx = buf.find(line_delimiter_bytes)
                if idx != -1:
                    # find next line delimiter
                    start_offset += idx + delimiter_len
                    found_next_row = True
                else:
                    max_read_length -= rlen
                    start_offset += rlen

            if not found_next_row:
                # Not find line delimiter to the end of file.
                # set start_offset to the end of file
                start_offset = file_size

        # 2. Handle end offset
        if end_offset != file_size:
            # Not the last chunk, step backwards a little,
            # in case end_offset point to the middle of the line delimiter.
            readable_end_offset = end_offset - 1
            readable_end_offset = max(readable_end_offset - delimiter_len - 1, 0)

            found_next_row = False
            max_read_length = file_size - readable_end_offset
            os.lseek(rfd, readable_end_offset, 0)

            while (not found_next_row) and max_read_length > 0:
                rlen = min(4096, max_read_length)
                buf = os.read(rfd, rlen)
                idx = buf.find(line_delimiter_bytes)
                if idx != -1:
                    # found next line delimiter
                    # point readable_end_offset to the end of next line delimiter
                    readable_end_offset += idx + delimiter_len - 1
                    found_next_row = True
                else:
                    max_read_length -= rlen
                    readable_end_offset += rlen

            if not found_next_row:
                # Not find line delimiter to the end of file.
                # set start_offset to the end of file
                end_offset = file_size
            else:
                end_offset = readable_end_offset + 1
 
        read_length = end_offset - start_offset
        os.lseek(rfd, start_offset, 0)
        while read_length > 0:
            rlen = min(4096, read_length)
            buf = os.read(rfd, rlen)
            sys.stdout.buffer.write(buf)
            read_length -= rlen
 
    except Exception as err:
        traceback.print_exc()
        sys.stderr.write('error: %s' % str(err))
        return -1
    finally:
        os.close(rfd)
 
    return 0

def parse_line_delimiter(line_delimiter):
    if line_delimiter.upper().startswith('\\X'):
        tmp = line_delimiter[2:]
        if tmp == "" or (len(tmp) % 2 != 0):
            sys.stderr.write('error: invalid line delimiter: %s\n' % line_delimiter)
            sys.exit(-1)

        for each_char in tmp:
            if HEX_STR.find(each_char.upper()) == -1:
                sys.stderr.write('error: invalid line delimiter: %s\n' % line_delimiter)
                sys.exit(-1)

        tmp_bytearray = bytearray.fromhex(tmp)
        line_delimiter = tmp_bytearray.decode('utf-8')
    else:
        line_delimiter = line_delimiter.replace('\\a', '\n')
        line_delimiter = line_delimiter.replace('\\b', '\b')
        line_delimiter = line_delimiter.replace('\\f', '\f')
        line_delimiter = line_delimiter.replace('\\n', '\n')
        line_delimiter = line_delimiter.replace('\\r', '\r')
        line_delimiter = line_delimiter.replace('\\t', '\t')
        line_delimiter = line_delimiter.replace('\\v', '\v')
        line_delimiter = line_delimiter.replace('\\\\', '\\')
        line_delimiter = line_delimiter.replace('\\\'', '\'')
        line_delimiter = line_delimiter.replace('\\\"', '\"')
        line_delimiter = line_delimiter.replace('\\?', '\?')
        line_delimiter = line_delimiter.replace('\\0', '\0')
    return line_delimiter
 
def parse_args(args):
    if len(args) == 0:
        usage()
        sys.exit(0)
 
    opts = ()
    file_name = ''
    chunk_num = 0
    chunk_idx = 0
    line_delimiter = ''
 
    try:
        opts, args = getopt.getopt(args, 'f:c:s:d:h')
    except getopt.GetoptError as err:
        sys.stderr.write('error: %s\n' % str(err))
        sys.exit(-1)
 
    if len(opts) != 4:
        usage()
        sys.exit(-1)
 
    for option, value in opts:
        if option == '-f':
            file_name = value
        elif option == '-c':
            chunk_num = value
        elif option == '-s':
            chunk_idx = value
        elif option == '-d':
            line_delimiter = parse_line_delimiter(value)
            # print(line_delimiter)
            # sys.exit(0)
 
    return (file_name, chunk_num, chunk_idx, line_delimiter)
 
if __name__ == '__main__':
    ret = split_output(*parse_args(sys.argv[1:]))
    sys.exit(ret)
