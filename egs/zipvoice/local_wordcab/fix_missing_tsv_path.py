#!/usr/bin/env python3
import argparse
import csv
csv.field_size_limit(2_147_483_647)
import os
import sys
from tempfile import NamedTemporaryFile
from shutil import move

def clean_tsv(infile: str, path_col: int,
              inplace: bool, backup_suffix: str,
              dry_run: bool, verbose: bool):
    if not os.path.isfile(infile):
        print(f"ERROR: File not found: {infile}", file=sys.stderr)
        return 1, 0, 0, 0

    outfh = None
    tmpfile = None
    if inplace and not dry_run:
        tmp = NamedTemporaryFile("w", delete=False, newline="", encoding="utf-8")
        outfh = tmp
        tmpfile = tmp.name
    else:
        outfh = sys.stdout

    kept, dropped, skipped = 0, 0, 0
    with open(infile, "r", newline="", encoding="utf-8") as infh:
        reader = csv.reader(infh, delimiter="\t")
        writer = csv.writer(outfh, delimiter="\t", lineterminator="\n")

        for lineno, row in enumerate(reader, start=1):
            if not row or len(row) <= path_col:
                skipped += 1  # skip malformed/empty rows
                continue

            path = row[path_col].strip()
            ok = False
            if path and os.path.isfile(path):
                try:
                    if os.stat(path).st_size > 0:
                        ok = True
                except OSError:
                    pass

            if ok:
                kept += 1
                writer.writerow(row)
            else:
                dropped += 1
                if verbose:
                    print(f"{infile}:{lineno}: DROP (missing/empty): {path}", file=sys.stderr)

    if inplace and not dry_run:
        if backup_suffix:
            backup_path = infile + backup_suffix
            try:
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                move(infile, backup_path)
            except Exception as e:
                print(f"ERROR: could not create backup '{backup_path}': {e}", file=sys.stderr)
        try:
            move(tmpfile, infile)
        except Exception as e:
            print(f"ERROR: could not replace '{infile}' with temp file: {e}", file=sys.stderr)

    print(f"{infile}: kept={kept}, dropped={dropped}, skipped={skipped}", file=sys.stderr)
    return 0, kept, dropped, skipped

def main():
    ap = argparse.ArgumentParser(
        description="Remove rows from TSV where the audio file is missing or 0 bytes."
    )
    ap.add_argument("tsv", nargs="+", help="TSV file(s) to process.")
    ap.add_argument("--path-col", type=int, default=2,
                    help="Zero-based index of path column (default: 2).")
    ap.add_argument("--inplace", action="store_true",
                    help="Edit TSVs in place (default: write to stdout).")
    ap.add_argument("--backup-suffix", default=".bak",
                    help="Suffix for backups when --inplace (default: .bak, '' disables).")
    ap.add_argument("--dry-run", action="store_true",
                    help="Scan but do not modify files.")
    ap.add_argument("-v", "--verbose", action="store_true",
                    help="Verbose logging to stderr.")
    args = ap.parse_args()

    overall_rc = 0
    total_kept = 0
    total_dropped = 0
    total_skipped = 0

    for tsv in args.tsv:
        rc, kept, dropped, skipped = clean_tsv(
            infile=tsv,
            path_col=args.path_col,
            inplace=args.inplace,
            backup_suffix=args.backup_suffix,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
        overall_rc |= rc
        total_kept += kept
        total_dropped += dropped
        total_skipped += skipped

    considered = total_kept + total_dropped
    pct_ok = (total_kept / considered * 100.0) if considered else 0.0
    pct_drop = (total_dropped / considered * 100.0) if considered else 0.0

    # Final summary to stderr
    print("\n=== Final statistics ===", file=sys.stderr)
    print(f"Files processed: {len(args.tsv)}", file=sys.stderr)
    print(f"Rows considered (kept + dropped): {considered}", file=sys.stderr)
    print(f"  kept:    {total_kept} ({pct_ok:.1f}%)", file=sys.stderr)
    print(f"  dropped: {total_dropped} ({pct_drop:.1f}%)", file=sys.stderr)
    print(f"Skipped (empty/malformed): {total_skipped}", file=sys.stderr)

    sys.exit(overall_rc)

if __name__ == "__main__":
    main()

