#!/usr/bin/env python3
import argparse
import csv
import os
import re
import shutil
import subprocess
import sys

def check_with_ffprobe(path):
    """Return (ok, details) where ok=True if mono/24k/pcm_s16le."""
    try:
        # streams info (channels, sample_rate, codec_name)
        # -v error keeps output clean if ffprobe is present
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "a:0",
            "-show_entries", "stream=channels,sample_rate,codec_name",
            "-of", "default=nw=1:nk=1",
            path,
        ]
        out = subprocess.check_output(cmd, text=True).strip().splitlines()
        # Expected order (one value per line): channels, sample_rate, codec_name
        if len(out) < 3:
            return (False, f"ffprobe: unexpected output {out!r}")
        channels = int(out[0])
        sample_rate = int(out[1])
        codec_name = out[2].strip().lower()
        ok = (channels == 1 and sample_rate == 24000 and codec_name == "pcm_s16le")
        return (ok, f"channels={channels}, rate={sample_rate}, codec={codec_name}")
    except FileNotFoundError:
        return (False, "ffprobe not found")
    except subprocess.CalledProcessError as e:
        return (False, f"ffprobe error: {e}")
    except Exception as e:
        return (False, f"ffprobe parse error: {e}")

def validate_file(tsv, path_col, new_root, use_ffprobe, quiet):
    errors = 0
    warnings = 0
    checked = 0
    new_root = new_root.rstrip("/")

    with open(tsv, "r", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh, delimiter="\t")
        for lineno, row in enumerate(reader, start=1):
            if not row or len(row) <= path_col:
                continue
            p = row[path_col].strip()
            checked += 1

            # Basic checks
            if not p.startswith("/"):
                print(f"{tsv}:{lineno}: ERROR: path not absolute: {p}", file=sys.stderr)
                errors += 1

            if "//" in p:
                print(f"{tsv}:{lineno}: WARN: path contains double slashes: {p}", file=sys.stderr)
                warnings += 1

            if not p.lower().endswith(".wav"):
                print(f"{tsv}:{lineno}: ERROR: not a .wav path: {p}", file=sys.stderr)
                errors += 1

            if not p.startswith(new_root + "/"):
                print(f"{tsv}:{lineno}: ERROR: not under new root '{new_root}': {p}", file=sys.stderr)
                errors += 1

            if not os.path.isfile(p):
                print(f"{tsv}:{lineno}: ERROR: file does not exist: {p}", file=sys.stderr)
                errors += 1
                continue  # ffprobe would fail anyway

            # Optional ffprobe verification
            if use_ffprobe:
                if shutil.which("ffprobe") is None:
                    print("ERROR: ffprobe not found in PATH (skip codec check).", file=sys.stderr)
                    use_ffprobe = False
                else:
                    ok, details = check_with_ffprobe(p)
                    if not ok:
                        print(f"{tsv}:{lineno}: ERROR: ffprobe check failed ({details}): {p}", file=sys.stderr)
                        errors += 1
                    elif not quiet:
                        print(f"{tsv}:{lineno}: OK: {details}: {p}", file=sys.stderr)

    return checked, warnings, errors

def main():
    ap = argparse.ArgumentParser(
        description="Validate TSV audio paths after media -> media_wav_24k migration."
    )
    ap.add_argument("tsv", nargs="+", help="TSV file(s) to validate.")
    ap.add_argument("--path-col", type=int, default=2, help="Zero-based path column (default: 2).")
    ap.add_argument("--new-root", default="/srv/speechcatcher/en_au/media_wav_24k",
                    help="Expected new root for audio files (default: %(default)s).")
    ap.add_argument("--ffprobe", action="store_true",
                    help="Use ffprobe to verify mono/24kHz/pcm_s16le.")
    ap.add_argument("-q", "--quiet", action="store_true", help="Less verbose when ffprobe is used.")
    args = ap.parse_args()

    total_checked = total_warn = total_err = 0
    for tsv in args.tsv:
        checked, warns, errs = validate_file(
            tsv, args.path_col, args.new_root, args.ffprobe, args.quiet
        )
        total_checked += checked
        total_warn += warns
        total_err += errs

    print(f"\nChecked rows: {total_checked} | Warnings: {total_warn} | Errors: {total_err}")
    sys.exit(1 if total_err else 0)

if __name__ == "__main__":
    main()

