#!/usr/bin/env python3
import argparse
import csv
import os
import sys
import signal
from pathlib import PurePosixPath
from tempfile import NamedTemporaryFile
from shutil import move

# Exit quietly when the downstream pipe (e.g., `head`) closes
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

def normalize_path(p: str) -> str:
    """Collapse multiple slashes while preserving a leading '/' or '//'."""
    if p.startswith("//"):
        prefix, rest = "//", p[2:]
    elif p.startswith("/"):
        prefix, rest = "/", p[1:]
    else:
        prefix, rest = "", p
    rest = "/".join(seg for seg in rest.split("/") if seg != "")
    return prefix + rest

def transform_path(path: str, old_root: str, new_root: str, force_ext: str = ".wav") -> str:
    """
    Replace old_root prefix with new_root and force extension to `force_ext`.
    Also normalizes duplicate slashes.
    """
    original = path.strip()
    if not original:
        return original

    old_root_norm = normalize_path(old_root.rstrip("/"))
    new_root_norm = normalize_path(new_root.rstrip("/"))
    p_norm = normalize_path(original)

    if old_root_norm and p_norm.startswith(old_root_norm):
        replaced = new_root_norm + p_norm[len(old_root_norm):]
    else:
        # Fallback: replace '/media/' component if old_root wasn't an exact prefix
        replaced = p_norm.replace("/media/", "/media_wav_24k/")

    # Force .wav extension
    posix = PurePosixPath(replaced)
    new_name = posix.stem + force_ext
    replaced = str(posix.with_name(new_name))

    return normalize_path(replaced)

def patch_tsv(
    infile: str,
    path_col: int,
    old_root: str,
    new_root: str,
    inplace: bool,
    backup_suffix: str,
    dry_run: bool,
    verbose: bool,
):
    if not os.path.isfile(infile):
        print(f"ERROR: File not found: {infile}", file=sys.stderr)
        return 1

    outfh = None
    tmpfile = None
    if inplace and not dry_run:
        tmp = NamedTemporaryFile("w", delete=False, newline="", encoding="utf-8")
        outfh = tmp
        tmpfile = tmp.name
    else:
        outfh = sys.stdout

    exit_code = 0
    with open(infile, "r", newline="", encoding="utf-8") as infh:
        reader = csv.reader(infh, delimiter="\t")
        writer = csv.writer(outfh, delimiter="\t", lineterminator="\n")

        for lineno, row in enumerate(reader, start=1):
            if not row:
                writer.writerow(row)
                continue

            if len(row) <= path_col:
                if verbose:
                    print(f"WARNING: {infile}:{lineno}: expected ≥ {path_col+1} columns, got {len(row)}; unchanged.", file=sys.stderr)
                writer.writerow(row)
                continue

            old_path = row[path_col]
            new_path = transform_path(old_path, old_root=old_root, new_root=new_root, force_ext=".wav")

            if verbose and new_path != old_path:
                print(f"{infile}:{lineno}: {old_path} -> {new_path}", file=sys.stderr)

            row[path_col] = new_path
            try:
                writer.writerow(row)
            except BrokenPipeError:
                # In case SIGPIPE handling didn’t kick in on some platforms
                return exit_code

    if inplace and not dry_run:
        if backup_suffix:
            backup_path = infile + backup_suffix
            try:
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                move(infile, backup_path)
            except Exception as e:
                print(f"ERROR: could not create backup '{backup_path}': {e}", file=sys.stderr)
                exit_code = 1
        try:
            move(tmpfile, infile)
        except Exception as e:
            print(f"ERROR: could not replace '{infile}' with temp file: {e}", file=sys.stderr)
            exit_code = 1

    return exit_code

def main():
    parser = argparse.ArgumentParser(
        description="Patch TSV files to point audio paths from media -> media_wav_24k and force .wav extension."
    )
    parser.add_argument("tsv", nargs="+", help="TSV file(s) to patch.")
    parser.add_argument("--path-col", type=int, default=2, help="Zero-based index of the path column (default: 2).")
    parser.add_argument("--old-root", default="/srv/speechcatcher/en_au/media",
                        help="Old media root to replace (default: /srv/speechcatcher/en_au/media).")
    parser.add_argument("--new-root", default="/srv/speechcatcher/en_au/media_wav_24k",
                        help="New media root (default: /srv/speechcatcher/en_au/media_wav_24k).")
    parser.add_argument("--inplace", action="store_true", help="Edit files in place (default: False; prints to stdout).")
    parser.add_argument("--backup-suffix", default=".bak",
                        help="Suffix for backups when --inplace is used (default: .bak; use '' to disable).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would change but do not modify files (works with --inplace).")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print per-line changes to stderr.")
    args = parser.parse_args()

    overall = 0
    for tsv in args.tsv:
        rc = patch_tsv(
            infile=tsv,
            path_col=args.path_col,
            old_root=args.old_root,
            new_root=args.new_root,
            inplace=args.inplace,
            backup_suffix=args.backup_suffix,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
        overall |= rc
    sys.exit(overall)

if __name__ == "__main__":
    main()

