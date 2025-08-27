#!/usr/bin/env python3
import argparse
import gzip
import json
import shutil
import time
from collections import Counter
from typing import Iterable, TextIO

def open_reader(path: str, gz: bool) -> Iterable[str]:
    """Yield lines from a text file, gzipped if gz=True."""
    if gz:
        fh = gzip.open(path, "rt", encoding="utf-8")
    else:
        fh = open(path, "r", encoding="utf-8")
    try:
        for line in fh:
            yield line
    finally:
        fh.close()

def open_writer(path: str, gz: bool) -> TextIO:
    """Open a text writer, gzipped if gz=True."""
    if gz:
        return gzip.open(path, "wt", encoding="utf-8")
    return open(path, "w", encoding="utf-8")

def main():
    parser = argparse.ArgumentParser(
        description="Count or fix ZipVoice manifests based on sampling_rate and channel."
    )
    parser.add_argument("manifest", help="Path to JSONL or JSONL.GZ manifest file")
    parser.add_argument(
        "--fix", action="store_true",
        help="Filter manifest to only entries matching target sampling_rate and channel, "
             "backup original to <file>.<timestamp>.bak, and write filtered to original filename."
    )
    parser.add_argument("--target-sampling-rate", type=int, default=24000,
                        help="Target sampling_rate to keep (default: 24000)")
    parser.add_argument("--target-channel", type=str, default="[0]",
                        help='Target channel list to keep, as JSON string (default: "[0]")')

    args = parser.parse_args()

    # Determine if input is gzipped based on filename.
    input_is_gz = args.manifest.endswith(".gz")

    # Parse target channel JSON
    try:
        target_channel = json.loads(args.target_channel)
        if not isinstance(target_channel, list):
            raise ValueError
    except Exception:
        parser.error('--target-channel must be a JSON list, e.g. "[0]" or "[0,1]".')

    sampling_rates = Counter()
    channels = Counter()
    total = 0
    kept = 0

    if args.fix:
        # Move original to backup (preserves compressed bytes if gz)
        backup_path = f"{args.manifest}.{int(time.time())}.bak"
        shutil.move(args.manifest, backup_path)

        # Read from backup, write filtered to original filename
        out_fh = open_writer(args.manifest, gz=input_is_gz)
        try:
            for raw_line in open_reader(backup_path, gz=input_is_gz):
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid JSON line: {e}")
                    continue

                sr = obj.get("recording", {}).get("sampling_rate")
                ch = obj.get("channel")

                # normalize channel for counting
                if isinstance(ch, list):
                    ch_key = tuple(ch)
                elif ch is None:
                    ch_key = None
                else:
                    # if it's a single int, normalize to list form for comparison
                    ch_key = (ch,)

                if sr is not None:
                    sampling_rates[sr] += 1
                if ch_key is not None:
                    channels[ch_key] += 1

                total += 1

                # Keep only matching entries
                if sr == args.target_sampling_rate and ch == target_channel:
                    kept += 1
                    out_fh.write(json.dumps(obj) + "\n")
        finally:
            out_fh.close()
    else:
        # Just count, no modification
        for raw_line in open_reader(args.manifest, gz=input_is_gz):
            line = raw_line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Skipping invalid JSON line: {e}")
                continue

            sr = obj.get("recording", {}).get("sampling_rate")
            ch = obj.get("channel")

            if isinstance(ch, list):
                ch_key = tuple(ch)
            elif ch is None:
                ch_key = None
            else:
                ch_key = (ch,)

            if sr is not None:
                sampling_rates[sr] += 1
            if ch_key is not None:
                channels[ch_key] += 1

            total += 1

    # Output stats
    print("=== Sampling Rates ===")
    for k in sorted(sampling_rates):
        print(f"{k}\t{sampling_rates[k]}")

    print("\n=== Channels ===")
    for k in sorted(channels):
        print(f"{list(k)}\t{channels[k]}")

    if args.fix:
        print(f"\nOriginal manifest moved to: {backup_path}")
        print(f"Filtered manifest written to: {args.manifest}")
        pct = (kept / total * 100) if total else 0.0
        print(f"Kept {kept} / {total} entries ({pct:.2f}%) "
              f"matching sampling_rate={args.target_sampling_rate}, channel={target_channel}")

if __name__ == "__main__":
    main()

