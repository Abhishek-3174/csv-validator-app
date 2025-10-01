#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import csv
import sys
import tempfile
import heapq
import os
from math import inf
from pathlib import Path
from typing import List, Tuple, TextIO

CSV_DELIM = ';'
SORT_CHUNK_SIZE = 500_000
DIFF_PATH = Path('normalized_diff.txt')
PROGRESS_INTERVAL = 1_000_000
EMOJI = {'ok': '✔', 'fail': '✘', 'warn': '⚠', 'info': 'ℹ', 'critical': '🚩'}

def printer(msg: str, level: str = 'info') -> None:
    print(f"{EMOJI.get(level, '')} {msg}")

def read_header(path: Path) -> List[str]:
    with path.open(newline='', encoding='utf-8-sig') as f:
        r = csv.reader(f, delimiter=CSV_DELIM)
        try:
            return next(r)
        except StopIteration:
            printer(f"File '{path.name}' is empty", 'fail')
            sys.exit(1)

def sort_csv_by_pk(in_path: Path, out_path: Path, key_idxs: List[int]) -> None:
    with in_path.open(newline='', encoding='utf-8-sig') as f:
        r = csv.reader(f, delimiter=CSV_DELIM)
        header = next(r, [])
        rows: List[List[str]] = []
        tmp_files: List[TextIO] = []

        for row in r:
            rows.append(row)
            if len(rows) >= SORT_CHUNK_SIZE:
                rows.sort(key=lambda x: tuple(x[i] for i in key_idxs))
                tf = tempfile.NamedTemporaryFile('w+', delete=False, newline='', encoding='utf-8')
                csv.writer(tf, delimiter=CSV_DELIM).writerows(rows)
                tf.flush(); tf.seek(0)
                tmp_files.append(tf)
                rows.clear()

        if rows:
            rows.sort(key=lambda x: tuple(x[i] for i in key_idxs))
            tf = tempfile.NamedTemporaryFile('w+', delete=False, newline='', encoding='utf-8')
            csv.writer(tf, delimiter=CSV_DELIM).writerows(rows)
            tf.flush(); tf.seek(0)
            tmp_files.append(tf)
            rows.clear()

    with out_path.open('w', newline='', encoding='utf-8') as outf:
        w = csv.writer(outf, delimiter=CSV_DELIM)
        w.writerow(header)
        iters = [csv.reader(tf, delimiter=CSV_DELIM) for tf in tmp_files]
        heap: List[Tuple[Tuple[str, ...], int, List[str]]] = []
        for idx, it in enumerate(iters):
            try:
                row = next(it)
                heap.append((tuple(row[i] for i in key_idxs), idx, row))
            except StopIteration:
                pass
        heapq.heapify(heap)

        while heap:
            _, idx, row = heapq.heappop(heap)
            w.writerow(row)
            try:
                nxt = next(iters[idx])
                heapq.heappush(heap, (tuple(nxt[i] for i in key_idxs), idx, nxt))
            except StopIteration:
                pass

    for tf in tmp_files:
        tf.close()
        os.unlink(tf.name)

class PKBlockReader:
    def __init__(self, f: TextIO, pk_cols: List[int]):
        self.reader = csv.reader(f, delimiter=CSV_DELIM)
        next(self.reader, None)
        self.pk_cols = pk_cols
        self.buffer = None
        self.exhausted = False
        self._advance()

    def _advance(self):
        try:
            self.buffer = next(self.reader)
        except StopIteration:
            self.buffer = None
            self.exhausted = True

    def get_next_block(self) -> Tuple[Tuple[str, ...], List[List[str]]] or None:
        if self.exhausted:
            return None
        current_pk = tuple(self.buffer[i] for i in self.pk_cols)
        block = []
        while self.buffer is not None:
            pk = tuple(self.buffer[i] for i in self.pk_cols)
            if pk != current_pk:
                break
            block.append(self.buffer)
            self._advance()
        return (current_pk, block)

def compute_row_cost(row1: List[str], row2: List[str], header1: List[str], header2: List[str]) -> int:
    idx_map2 = [header2.index(c) if c in header2 else None for c in header1]
    aligned2 = [row2[m] if m is not None else '' for m in idx_map2]
    return sum(1 for c1, c2 in zip(row1, aligned2) if c1 != c2)

def gather_column_mismatches(row1: List[str], row2: List[str], h1: List[str], h2: List[str]) -> List[Tuple[str,str,str]]:
    idx_map2 = [h2.index(c) if c in h2 else None for c in h1]
    aligned2 = [row2[m] if m is not None else '' for m in idx_map2]
    return [(h1[i], row1[i], aligned2[i]) for i in range(len(h1)) if row1[i] != aligned2[i]]

def hungarian_algorithm(cost_matrix: List[List[int]]) -> List[Tuple[int,int]]:
    n = len(cost_matrix)
    m = max(len(r) for r in cost_matrix) if n else 0
    if n == 0 or m == 0:
        return []

    size = max(n, m)
    big_cost = 10_000_000
    padded = [[0]*size for _ in range(size)]

    for i in range(n):
        row_len = len(cost_matrix[i])
        for j in range(row_len):
            padded[i][j] = cost_matrix[i][j]
        for j in range(row_len, size):
            padded[i][j] = big_cost
    for i in range(n, size):
        for j in range(size):
            padded[i][j] = big_cost

    for i in range(size):
        row_min = min(padded[i])
        for j in range(size):
            padded[i][j] -= row_min

    for j in range(size):
        col_min = min(padded[i][j] for i in range(size))
        for i in range(size):
            padded[i][j] -= col_min

    marked = [[-1]*size for _ in range(size)]
    row_covered = [False]*size
    col_covered = [False]*size

    for i in range(size):
        for j in range(size):
            if padded[i][j] == 0 and not row_covered[i] and not col_covered[j]:
                marked[i][j] = 0
                row_covered[i] = True
                col_covered[j] = True
                break
    row_covered = [False]*size
    col_covered = [False]*size

    def find_star_in_row(row):
        for c in range(size):
            if marked[row][c] == 0:
                return c
        return None

    def find_star_in_col(col):
        for r in range(size):
            if marked[r][col] == 0:
                return r
        return None

    def find_prime_in_row(row):
        for c in range(size):
            if marked[row][c] == 1:
                return c
        return None

    def find_a_zero():
        for rr in range(size):
            if not row_covered[rr]:
                for cc in range(size):
                    if padded[rr][cc] == 0 and not col_covered[cc]:
                        return rr, cc
        return None

    def cover_cols_with_stars():
        for rr in range(size):
            for cc in range(size):
                if marked[rr][cc] == 0:
                    col_covered[cc] = True

    def augment_path(r, c):
        path = [(r, c)]
        while True:
            sr = find_star_in_col(path[-1][1])
            if sr is None:
                break
            path.append((sr, path[-1][1]))
            pc = find_prime_in_row(sr)
            path.append((sr, pc))
        for (rr, cc) in path:
            if marked[rr][cc] == 0:
                marked[rr][cc] = -1
            elif marked[rr][cc] == 1:
                marked[rr][cc] = 0

    step = 4

    while True:
        if step == 4:
            cover_cols_with_stars()
            if sum(col_covered) == size:
                break
            step = 5
        elif step == 5:
            while True:
                z = find_a_zero()
                if z is None:
                    step = 6
                    break
                rr, cc = z
                marked[rr][cc] = 1
                sc = find_star_in_row(rr)
                if sc is not None:
                    row_covered[rr] = True
                    col_covered[sc] = False
                else:
                    augment_path(rr, cc)
                    row_covered = [False]*size
                    col_covered = [False]*size
                    for i in range(size):
                        for j in range(size):
                            if marked[i][j] == 1:
                                marked[i][j] = -1
                    step = 4
                    break
        elif step == 6:
            val = inf
            for rr in range(size):
                if not row_covered[rr]:
                    for cc in range(size):
                        if not col_covered[cc]:
                            val = min(val, padded[rr][cc])
            if val == inf:
                break
            for rr in range(size):
                if row_covered[rr]:
                    for cc in range(size):
                        padded[rr][cc] += val
            for cc in range(size):
                if not col_covered[cc]:
                    for rr in range(size):
                        padded[rr][cc] -= val
            step = 4
        else:
            break

    results = []
    for i in range(n):
        for j in range(len(cost_matrix[i])):
            if marked[i][j] == 0:
                results.append((i,j))
    return results

def match_blocks_hungarian(
    pk: Tuple[str, ...],
    block1: List[List[str]],
    block2: List[List[str]],
    h1: List[str],
    h2: List[str],
    diff_file,
    diff_cols: set
) -> Tuple[int, int, int]:
    if not block1 and not block2:
        return (0, 0, 0)
    if not block1:
        for r2 in block2:
            diff_file.write(f"EXTRA|PK={';'.join(pk)}|row={CSV_DELIM.join(r2)}\n")
        return (0, len(block2), 0)
    if not block2:
        for r1 in block1:
            diff_file.write(f"MISSING|PK={';'.join(pk)}|row={CSV_DELIM.join(r1)}\n")
        return (len(block1), 0, 0)

    cost_matrix = []
    for r1 in block1:
        row_costs = []
        for r2 in block2:
            row_costs.append(compute_row_cost(r1, r2, h1, h2))
        cost_matrix.append(row_costs)

    pairing = hungarian_algorithm(cost_matrix)

    matched1 = set(i for i,_ in pairing)
    matched2 = set(j for _,j in pairing)
    mismatch_rows = 0

    for (i,j) in pairing:
        mm = gather_column_mismatches(block1[i], block2[j], h1, h2)
        if mm:
            mismatch_rows += 1
        for (col_name, old_val, new_val) in mm:
            diff_file.write(f"MISMATCH|PKValue={';'.join(pk)}|column={col_name}|firstValue={old_val}|secondValue={new_val}\n")
            diff_cols.add(col_name)

    missing_count = 0
    for i in range(len(block1)):
        if i not in matched1:
            diff_file.write(f"MISSING|PKValue={';'.join(pk)}|row={CSV_DELIM.join(block1[i])}\n")
            missing_count += 1

    extra_count = 0
    for j in range(len(block2)):
        if j not in matched2:
            diff_file.write(f"EXTRA|PKValue={';'.join(pk)}|row={CSV_DELIM.join(block2[j])}\n")
            extra_count += 1

    return (missing_count, extra_count, mismatch_rows)

def single_pass_full_diff_with_duplicates(
    f1_sorted: Path,
    f2_sorted: Path,
    h1: List[str],
    h2: List[str],
    pk1: List[int],
    pk2: List[int],
    diff_output: Path
) -> Tuple[int,int,int,set]:
    missing_total = 0
    extra_total = 0
    mismatch_rows_total = 0
    diff_cols = set()

    with f1_sorted.open('r', newline='', encoding='utf-8') as fin1, \
         f2_sorted.open('r', newline='', encoding='utf-8') as fin2, \
         diff_output.open('w', encoding='utf-8') as out:

        r1 = PKBlockReader(fin1, pk1)
        r2 = PKBlockReader(fin2, pk2)
        block1 = r1.get_next_block()
        block2 = r2.get_next_block()
        line_count = 0

        while block1 is not None or block2 is not None:
            if block1 is None:
                pk2_val, rows2 = block2
                for row2 in rows2:
                    out.write(f"EXTRA|PKValue={';'.join(pk2_val)}|row={CSV_DELIM.join(row2)}\n")
                extra_total += len(rows2)
                block2 = r2.get_next_block()
                continue
            if block2 is None:
                pk1_val, rows1 = block1
                for row1 in rows1:
                    out.write(f"MISSING|PKValue={';'.join(pk1_val)}|row={CSV_DELIM.join(row1)}\n")
                missing_total += len(rows1)
                block1 = r1.get_next_block()
                continue

            pk1_val, rows1 = block1
            pk2_val, rows2 = block2

            if pk1_val == pk2_val:
                mc, ec, mr = match_blocks_hungarian(pk1_val, rows1, rows2, h1, h2, out, diff_cols)
                missing_total += mc
                extra_total += ec
                mismatch_rows_total += mr
                block1 = r1.get_next_block()
                block2 = r2.get_next_block()
            elif pk1_val < pk2_val:
                for row1 in rows1:
                    out.write(f"MISSING|PKValue={';'.join(pk1_val)}|row={CSV_DELIM.join(row1)}\n")
                missing_total += len(rows1)
                block1 = r1.get_next_block()
            else:
                for row2 in rows2:
                    out.write(f"EXTRA|PKValue={';'.join(pk2_val)}|row={CSV_DELIM.join(row2)}\n")
                extra_total += len(rows2)
                block2 = r2.get_next_block()

            line_count += 1
            if line_count % PROGRESS_INTERVAL == 0:
                printer(f"Processed ~{line_count:,} PK blocks in full diff...")

    return (missing_total, extra_total, mismatch_rows_total, diff_cols)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file1', type=Path)
    parser.add_argument('file2', type=Path)
    parser.add_argument('primary_key', nargs='?', help="Comma-separated PK columns (defaults to file1[0])")
    args = parser.parse_args()


    h1 = read_header(args.file1)
    h2 = read_header(args.file2)

    if args.primary_key:
        pk_col_names = [c.strip() for c in args.primary_key.split(',')]
    else:
        pk_col_names = [h1[0]]

    pk1 = []
    pk2 = []
    for c in pk_col_names:
        if c not in h1:
            printer(f"PK column '{c}' not in file1 header", 'fail')
            sys.exit(1)
        if c not in h2:
            printer(f"PK column '{c}' not in file2 header", 'fail')
            sys.exit(1)
        pk1.append(h1.index(c))
        pk2.append(h2.index(c))

    printer("Sorting files on PK...")
    s1 = args.file1.with_name(args.file1.stem + '.sorted.csv')
    s2 = args.file2.with_name(args.file2.stem + '.sorted.csv')
    sort_csv_by_pk(args.file1, s1, pk1)
    sort_csv_by_pk(args.file2, s2, pk2)
    printer("Sorting complete", "ok")

    missing_cols = sorted(set(h1) - set(h2))
    extra_cols = sorted(set(h2) - set(h1))
    if missing_cols or extra_cols:
        if missing_cols:
            printer("Columns missing in second: " + ", ".join(missing_cols), "critical")
        if extra_cols:
            printer("Columns extra in second: " + ", ".join(extra_cols), "critical")
    else:
        printer("No header differences found.", "info")
    printer("CHECK 1 (Header Differences) complete", "ok")

    printer("CHECK 2 (PK counts) + CHECK 3 (Data Diff) in single pass...")
    missing_count, extra_count, mismatch_rows_count, diff_cols = single_pass_full_diff_with_duplicates(
        s1, s2, h1, h2, pk1, pk2, DIFF_PATH
    )

    if missing_count > 0:
        printer(f"Missing rows in file2: {missing_count}", "critical")
    else:
        printer("No missing rows in file2", "info")

    if extra_count > 0:
        printer(f"Extra rows in file2:   {extra_count}", "critical")
    else:
        printer("No extra rows in file2", "info")

    if mismatch_rows_count > 0:
        printer(f"Mismatching rows: {mismatch_rows_count}", "critical")
    else:
        printer("No mismatching rows found.", "info")

    if diff_cols:
        printer("Columns with differing values: " + ", ".join(sorted(diff_cols)), "critical")
    else:
        printer("No columns with differing values.", "info")

    printer(f"Detailed output written to '{DIFF_PATH}'", "ok")

    try:
        os.remove(s1)
    except OSError:
        pass
    try:
        os.remove(s2)
    except OSError:
        pass

def run_diff(file1_path, file2_path, primary_key=None):
    import sys
    sys.argv = [
        "csv_diff_tool.py",
        str(file1_path),
        str(file2_path),
    ]
    if primary_key:
        sys.argv.append(primary_key)
    main()
