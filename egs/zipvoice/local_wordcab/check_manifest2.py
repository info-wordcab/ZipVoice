#!/usr/bin/env python3
import argparse
import gzip
import json
import os
import re
import shutil
import time
import unicodedata
from collections import Counter, defaultdict
from typing import Dict, Iterable, Iterator, Optional, Tuple

# ---------- IO helpers (strict UTF-8 with decode-error detection) ----------

def open_reader_bytes(path: str, gz: bool) -> Iterator[bytes]:
    """Yield raw lines (bytes) from file, supporting gzip."""
    fh = gzip.open(path, "rb") if gz else open(path, "rb")
    try:
        for line in fh:
            yield line
    finally:
        fh.close()

def open_writer(path: str, gz: bool):
    """Open a text writer, gzipped if gz=True."""
    return gzip.open(path, "wt", encoding="utf-8", newline="\n") if gz else open(path, "w", encoding="utf-8", newline="\n")

# ---------- Unicode normalization utilities ----------

_ZW_CHARS = (
    "\u200B"  # ZERO WIDTH SPACE
    "\u200C"  # ZERO WIDTH NON-JOINER
    "\u200D"  # ZERO WIDTH JOINER
    "\u2060"  # WORD JOINER
    "\u180E"  # MONGOLIAN VOWEL SEPARATOR (deprecated)
    "\uFEFF"  # ZERO WIDTH NO-BREAK SPACE (BOM)
)

# map common “fancy” punctuation -> ASCII
_PUNCT_MAP = {
    # quotes
    "“": '"', "”": '"', "„": '"', "‟": '"', "«": '"', "»": '"',
    "‘": "'", "’": "'", "‚": "'", "‹": "'", "›": "'",
    # dashes
    "–": "-", "—": "-", "―": "-",
    # ellipsis
    "…": "...",
    # misc punctuation/symbols to rough ASCII
    "•": "*", "·": "*", "‧": "*",
    "▪": "*", "◦": "*",
    "→": "->", "←": "<-", "↔": "<->", "⇒": "=>", "⇐": "<=",
    # rare spaces (handled below too, but safe to map)
    "\u00A0": " ",  # NBSP
    "\u2009": " ",  # THIN SPACE
    "\u200A": " ",  # HAIR SPACE
    "\u2008": " ",
    "\u2007": " ",
    "\u202F": " ",  # NARROW NBSP
    "\u205F": " ",
    "\u1680": " ",
    "\u180E": " ",  # legacy
    "\u2000": " ", "\u2001": " ", "\u2002": " ", "\u2003": " ",
    "\u2004": " ", "\u2005": " ", "\u2006": " ",
    "\u3000": " ",  # IDEOGRAPHIC SPACE
}

_PUNCT_TRANS = str.maketrans(_PUNCT_MAP)

_CTRL_REGEX = re.compile(
    # All Cc (control) or Cf (format) except allowed whitespace
    r"[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F"
    r"\u200B-\u200D\u2060\uFEFF]"
)

def _drop_controls(s: str) -> Tuple[str, int, int]:
    """Remove control chars; count (controls_removed, zw_removed)."""
    controls_removed = 0
    zw_removed = 0
    def repl(m):
        nonlocal controls_removed, zw_removed
        chs = m.group(0)
        for ch in chs:
            if ch in _ZW_CHARS:
                zw_removed += 1
            else:
                controls_removed += 1
        return ""
    return _CTRL_REGEX.sub(repl, s), controls_removed, zw_removed

_WS_MULTI = re.compile(r"[ \t\f\v]+")
_WS_LINES = re.compile(r"[ \t\f\v]*\n[ \t\f\v]*")  # trim spaces around newlines

def normalize_text(s: str) -> Tuple[str, Dict[str, int]]:
    """
    Normalize Unicode issues in free text.
    Returns (normalized_text, stats_dict)
    stats keys: replaced_punct, controls_removed, zero_width_removed, nbspace_to_space, whitespace_collapsed, nkfc_changes
    """
    stats = defaultdict(int)

    # 1) NFKC normalize
    before = s
    s = unicodedata.normalize("NFKC", s)
    if s != before:
        stats["nkfc_changes"] += sum(1 for a, b in zip(before, s) if a != b) + abs(len(before) - len(s))

    # 2) Replace fancy punctuation & odd spaces via mapping
    before = s
    s = s.translate(_PUNCT_TRANS)
    if s != before:
        # count approx how many mapped chars (difference by per-char membership)
        stats["replaced_punct"] += sum(1 for ch in before if ch in _PUNCT_MAP)

    # Count NBSP specifically (already translated above)
    stats["nbspace_to_space"] += before.count("\u00A0")

    # 3) Drop control and zero-width characters
    s, controls_removed, zw_removed = _drop_controls(s)
    stats["controls_removed"] += controls_removed
    stats["zero_width_removed"] += zw_removed

    # 4) Normalize whitespace
    before = s
    # collapse spaces/tabs/etc (not newlines)
    s = _WS_MULTI.sub(" ", s)
    # trim space around newlines
    s = _WS_LINES.sub("\n", s)
    # collapse multiple newlines to max 2
    s = re.sub(r"\n{3,}", "\n\n", s)
    if s != before:
        stats["whitespace_collapsed"] += 1

    # 5) Strip leading/trailing whitespace
    s_stripped = s.strip()
    if s_stripped != s:
        stats["whitespace_collapsed"] += 1
        s = s_stripped

    return s, stats

def _iter_lines_utf8(path: str, gz: bool) -> Iterator[Tuple[Optional[str], Optional[Exception]]]:
    """
    Yield (line, error). If the raw line fails to decode as UTF-8, returns (None, UnicodeDecodeError).
    """
    for raw in open_reader_bytes(path, gz=gz):
        try:
            yield raw.decode("utf-8"), None
        except UnicodeDecodeError as e:
            yield None, e

# ---------- Main program ----------

def main():
    parser = argparse.ArgumentParser(
        description="Count or fix ZipVoice manifests based on sampling_rate and channel, with Unicode cleanup."
    )
    parser.add_argument("manifest", help="Path to JSONL or JSONL.GZ manifest file")
    parser.add_argument(
        "--fix", action="store_true",
        help="Filter manifest to only entries matching target sampling_rate and channel, "
             "backup original to <file>.<timestamp>.bak, and write cleaned+filtered to original filename."
    )
    parser.add_argument("--target-sampling-rate", type=int, default=24000,
                        help="Target sampling_rate to keep (default: 24000)")
    parser.add_argument("--target-channel", type=str, default='[0]',
                        help='Target channel list to keep, as JSON string (default: "[0]")')
    parser.add_argument("--keep-empty-text", action="store_true",
                        help="By default, entries whose text becomes empty after normalization are kept. "
                             "Enable this to keep them anyway (useful if model can handle empty text).")
    parser.add_argument("--min-duration", type=float, default=3.0,
                        help="Minimum cut duration (in seconds) to keep (default: 3.0)")
    args = parser.parse_args()

    input_is_gz = args.manifest.endswith(".gz")

    # Parse target channel JSON
    try:
        target_channel = json.loads(args.target_channel)
        if not isinstance(target_channel, list):
            raise ValueError
    except Exception:
        parser.error('--target-channel must be a JSON list, e.g. "[0]" or "[0,1]".')

    # Counters
    sampling_rates = Counter()
    channels = Counter()
    total = 0
    kept = 0

    # Unicode statistics
    decode_errors = 0
    invalid_json = 0
    normalized_entries = 0
    empty_text_after_norm = 0
    too_short_duration = 0  # new: count entries rejected by min-duration

    # Aggregated char-level stats
    agg_norm_stats = Counter()

    # Output setup (only in --fix)
    if args.fix:
        backup_path = f"{args.manifest}.{int(time.time())}.bak"
        shutil.move(args.manifest, backup_path)
        in_path = backup_path
        out_fh = open_writer(args.manifest, gz=input_is_gz)
    else:
        in_path = args.manifest
        out_fh = None

    try:
        for line_str, err in _iter_lines_utf8(in_path, gz=input_is_gz):
            if err is not None:
                decode_errors += 1
                # skip undecodable line entirely
                continue

            line = line_str.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                invalid_json += 1
                continue

            # Gather base stats
            sr = obj.get("recording", {}).get("sampling_rate")
            dur = obj.get("duration")  # top-level duration of the cut
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

            # Clean unicode in all supervision texts
            sups = obj.get("supervisions") or []
            any_change = False
            entry_norm_stats = Counter()

            for sup in sups:
                txt = sup.get("text")
                if isinstance(txt, str):
                    new_txt, st = normalize_text(txt)
                    if new_txt != txt:
                        any_change = True
                    entry_norm_stats.update(st)
                    sup["text"] = new_txt

            if any_change:
                normalized_entries += 1
                agg_norm_stats.update(entry_norm_stats)

            # Optionally drop if ALL supervision texts are empty after normalization
            if sups:
                if all((not isinstance(s.get("text"), str)) or (s.get("text").strip() == "") for s in sups):
                    empty_text_after_norm += 1
                    # Usually it's safer to keep the cut but you might want to drop it.
                    # We'll keep by default unless user passes a flag. (Default behavior: keep.)
                    if not args.keep_empty_text:
                        # if we are not keeping empty text, treat as not matching (skip write), but still counted above
                        continue

            # Filter by min duration first (must be present and >= min-duration)
            meets_duration = (isinstance(dur, (int, float)) and dur >= args.min_duration)
            if not meets_duration:
                if isinstance(dur, (int, float)) and dur < args.min_duration:
                    too_short_duration += 1
                # If duration is missing or too short, skip
                continue

            # Filter by target sampling_rate and channel (same logic as before)
            if sr == args.target_sampling_rate and ch == target_channel:
                kept += 1
                if out_fh is not None:
                    # write cleaned version
                    out_fh.write(json.dumps(obj, ensure_ascii=False) + "\n")
            else:
                # not kept — do nothing
                pass
    finally:
        if out_fh is not None:
            out_fh.close()

    # ---------- Summary ----------
    print("=== Sampling Rates ===")
    for k in sorted(sampling_rates):
        print(f"{k}\t{sampling_rates[k]}")

    print("\n=== Channels ===")
    for k in sorted(channels):
        print(f"{list(k)}\t{channels[k]}")

    print("\n=== Unicode / Text Cleanup Summary ===")
    print(f"Lines with UTF-8 decode errors (skipped): {decode_errors}")
    print(f"Lines with invalid JSON (skipped):        {invalid_json}")
    print(f"Entries with normalized text:             {normalized_entries}")

    # Detail the normalization tallies
    if agg_norm_stats:
        def get(k): return agg_norm_stats.get(k, 0)
        print("  Character-level changes:")
        print(f"    nkfc_changes:         {get('nkfc_changes')}")
        print(f"    replaced_punct:       {get('replaced_punct')}")
        print(f"    nbspace_to_space:     {get('nbspace_to_space')}")
        print(f"    zero_width_removed:   {get('zero_width_removed')}")
        print(f"    controls_removed:     {get('controls_removed')}")
        print(f"    whitespace_norm_ops:  {get('whitespace_collapsed')}")

    print(f"\nEntries with empty text after normalization: {empty_text_after_norm}"
          f" ({'kept' if args.keep_empty_text else 'not kept when filtering condition fails'})")
    print(f"Entries skipped for duration < {args.min_duration:.2f}s: {too_short_duration}")

    if args.fix:
        pct = (kept / max(1, (total))) * 100.0
        print(f"\nOriginal manifest moved to: {backup_path}")
        print(f"Filtered+cleaned manifest written to: {args.manifest}")
        print(f"Kept {kept} / {total} entries ({pct:.2f}%) "
              f"matching sampling_rate={args.target_sampling_rate}, channel={target_channel}, "
              f"duration>={args.min_duration:.2f}s")
    else:
        print("\n(Run with --fix to write a cleaned, filtered manifest.)")

if __name__ == "__main__":
    main()

