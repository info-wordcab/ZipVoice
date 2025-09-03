#!/usr/bin/env python3
import argparse
import gzip
import io
import json
import os
import shutil
import sys
import tempfile
import time
from typing import TextIO, Tuple

def open_maybe_gzip(path: str, mode: str) -> TextIO:
    """
    Open plain or .gz files in text mode with UTF-8 encoding.
    mode should be 'r' or 'w'.
    """
    assert mode in ("r", "w")
    text_mode = "rt" if mode == "r" else "wt"
    if path.endswith(".gz"):
        return gzip.open(path, text_mode, encoding="utf-8")
    else:
        return open(path, text_mode, encoding="utf-8", newline="\n")

def compute_min_seconds(args) -> float:
    if args.min_seconds is not None:
        return args.min_seconds
    # Derive from STFT needs when using reflect/replicate padding.
    if args.pad_mode in ("reflect", "replicate"):
        # Need len(x) > n_fft//2 samples to avoid the error.
        required_samples = (args.n_fft // 2) + 1  # +1 to be strictly greater
        return required_samples / float(args.target_sr)
    # constant padding imposes no constraint
    return 0.0

def filter_line(obj: dict, min_seconds: float, check_supervisions: bool) -> bool:
    """
    Return True if the cut should be kept.
    Basic rule: keep if obj['duration'] >= min_seconds.
    Optionally also require *all* supervisions to meet the threshold.
    """
    try:
        if float(obj.get("duration", 0.0)) < min_seconds:
            return False
    except Exception:
        return False
    if check_supervisions:
        sups = obj.get("supervisions", [])
        for s in sups:
            try:
                if float(s.get("duration", 0.0)) < min_seconds:
                    return False
            except Exception:
                return False
    return True

def atomic_replace(src: str, dst: str):
    # On POSIX, os.replace is atomic within the same filesystem.
    os.replace(src, dst)

def main():
    p = argparse.ArgumentParser(
        description="Filter super-short utterances from a JSONL(.gz) ZipVoice/Lhotse-style manifest."
    )
    p.add_argument("input", help="Path to input .jsonl or .jsonl.gz manifest")
    p.add_argument(
        "-o", "--output",
        help="Path to output file (.jsonl or .jsonl.gz). "
             "Default: in-place (same as input, with a timestamped .bak created first)."
    )
    p.add_argument(
        "--min-seconds", type=float, default=None,
        help="Minimum duration (seconds) for a cut to be kept. "
             "If omitted, it's derived from --n-fft/--target-sr/--pad-mode."
    )
    p.add_argument(
        "--n-fft", type=int, default=1024,
        help="FFT size used in your STFT frontend (default: 1024)."
    )
    p.add_argument(
        "--target-sr", type=int, default=24000,
        help="Target sample rate expected by the model/frontend (default: 24000)."
    )
    p.add_argument(
        "--pad-mode", choices=["reflect", "replicate", "constant"], default="reflect",
        help="Padding mode used in STFT. Affects derived min-seconds (default: reflect)."
    )
    p.add_argument(
        "--check-supervisions", action="store_true",
        help="Also require every supervision.duration in each cut to be >= min-seconds."
    )
    p.add_argument(
        "--keep-empty", action="store_true",
        help="If all cuts are filtered out, still produce an empty output file (default: exit with error)."
    )
    args = p.parse_args()

    if not os.path.isfile(args.input):
        print(f"ERROR: Input not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    min_seconds = compute_min_seconds(args)

    # Determine output path behavior
    in_place = args.output is None or os.path.abspath(args.output) == os.path.abspath(args.input)
    output_path = args.input if in_place else args.output

    # Prepare backup if in-place
    backup_path = None
    if in_place:
        ts = int(time.time())
        backup_path = f"{args.input}.{ts}.bak"
        shutil.copy2(args.input, backup_path)
        print(f"Backup created: {backup_path}", file=sys.stderr)

    # Write to a temp file first, then replace/move
    out_dir = os.path.dirname(os.path.abspath(output_path)) or "."
    fd, tmp_path = tempfile.mkstemp(prefix=".filter_tmp_", dir=out_dir, text=True)
    os.close(fd)  # re-open with our text/gzip helper

    kept = 0
    total = 0
    try:
        with open_maybe_gzip(args.input, "r") as fin, open_maybe_gzip(tmp_path, "w") as fout:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                total += 1
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    # Skip malformed lines
                    continue
                if filter_line(obj, min_seconds=min_seconds, check_supervisions=args.check_supervisions):
                    fout.write(json.dumps(obj, ensure_ascii=False) + "\n")
                    kept += 1
    except Exception as e:
        # Clean up temp file on errors
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        print(f"ERROR during processing: {e}", file=sys.stderr)
        sys.exit(2)

    if kept == 0 and not args.keep_empty:
        # Do not modify the original if nothing remains, unless --keep-empty
        try:
            os.remove(tmp_path)
        except Exception:
            pass
        if backup_path and in_place:
            print("No cuts remained after filtering; original file left untouched. "
                  f"Backup is at: {backup_path}", file=sys.stderr)
        else:
            print("No cuts remained after filtering; not writing output.", file=sys.stderr)
        sys.exit(3)

    # If output extension is .gz but tmp_path is plain, we already wrote gzip via helper.
    # Now atomically replace/move.
    if in_place:
        atomic_replace(tmp_path, output_path)
        print(f"Wrote filtered manifest in-place to: {output_path}", file=sys.stderr)
    else:
        # Move tmp to output
        atomic_replace(tmp_path, output_path)
        print(f"Wrote filtered manifest to: {output_path}", file=sys.stderr)

    dropped = total - kept
    print(f"Summary: total={total}, kept={kept}, dropped={dropped}, "
          f"min_seconds={min_seconds:.6f}, pad_mode={args.pad_mode}, "
          f"n_fft={args.n_fft}, target_sr={args.target_sr}", file=sys.stderr)

if __name__ == "__main__":
    main()

