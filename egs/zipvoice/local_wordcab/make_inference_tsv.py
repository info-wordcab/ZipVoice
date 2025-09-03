#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Generate a .tsv file for ZipVoice inference.

Format (one item per line):
outputfilename_without_extension<TAB>prompt_transcription<TAB>prompt_wav<TAB>text_to_synthesize

Output filename convention: voicename_promptname (e.g., irwin_customspeech1)

Usage:
  python make_tsv.py               # writes test.tsv in the cwd
  python make_tsv.py -o my.tsv     # custom output path
"""

import argparse
from collections import OrderedDict
from pathlib import Path

# ----------------------------
# Hardcoded data (edit freely)
# ----------------------------


# Voices: each has a 'name', a 'prompt_wav' path, and the 'prompt_transcription'
VOICES = [
    {
        "name": "irwin",
        "prompt_wav": "irwin.wav",
        "prompt_transcription": (
            "The seed that the parrot kicks out of its cage attracts mice and mice are what the snake's here for. "
            "Try a different kind of cage and hey, no mice and no snakes. With me out of the way, the snake comes out "
            "to look for breakfast. Mice find their food by sniffing the air. Mice use scent too, but they don't use their nostrils."
        ),
    },
    {
        "name": "albonese",
        "prompt_wav": "albonese.wav",
        "prompt_transcription": (
            "But as a fundamental priority for our economy and our society. And Australians voted for a Labor government "
            "to keep delivering real and lasting help with their cost of living."
        ),
    },
    {
        "name": "david",
        "prompt_wav": "david.wav",
        "prompt_transcription": (
            "Thanks, everyone. So, again, yeah, look, let's get into it. We're here today to talk about Big Tin Can's view "
            "of how we can create what we call a single pane of glass, the employee store experience, with the simple goal "
            "that we want every human."
        ),
    },
    {
        "name": "cate_blanchett",
        "prompt_wav": "cate_blanchett.wav",
        "prompt_transcription": (
            "And a huge admirer of Alphonsus for a long time. And we'd met tangentially, but seriously. "
            "And then he called and said that he was going to make a serialized film, is how he described it to me, "
            "in seven parts, and would I be part of it? And so he said, I don't want to tell you anything before you read it, "
            "but just once you've read it, get back to me. And I sort of, it was one of those ones that I..."
        ),
    },
    {
        "name": "hugh_jackman",
        "prompt_wav": "hugh_jackman.wav",
        "prompt_transcription": (
            "No, I remember, I just remember being blown away by you being there. I was so excited to meet you and "
            "so thrilled you were there and we needed you. I remember that day very well. "
            "We returned a compliment, we're starting being sincere. So I remember one day I saw you sitting on the steps "
            "of your trailer at the end of the day, and I said you're right. You know, so he goes I could have done that better, "
            "over expressive. That as opposed to that is a laugh. Yeah, yeah. And."
        ),
    },
    {
        "name": "normie",
        "prompt_wav": "normie0.wav",
        "prompt_transcription": (
            "Yeah, I'm really happy with how this is potting up, Mark. It's actually coming along really well. "
            "A really good level of pods. So it's probably got another, I'm hoping, at least a month of flowering. "
            "So flowering's good, that's how we're making seeds. So yeah, should hopefully be successful. Maybe one point five, maybe a little better."
        ),
    },
]


PROMPTS = OrderedDict(
    [
        (
            "customspeech1",
            "Good day mates! You're listenin' to ZipVoice - smooth as a roo hoppin' at sunrise. "
            "The Australian outback covers nearly seventy percent of the continent, but less than ten percent of Aussies actually live there. "
            "It’s home to wild camels, giant red kangaroos, and some of the clearest starry skies on Earth.",
        ),
        (
            "customspeech1b",
            "You're listenin' to ZipVoice - smooth as a roo hoppin' at sunrise. "
            "The Australian outback covers nearly seventy percent of the continent, but less than ten percent of Aussies actually live there. "
            "It’s home to wild camels, giant red kangaroos, and some of the clearest starry skies on Earth.",
        ),
        (
            "customspeech2",
            "Hey there, just calling about my insurance plan, and, uh, wanted to see if it covers my condition, you know? "
            "I've been getting symptoms and my doctor wanted me to get this procedure done by Friday. "
            "So yeah, can you please let me know - what... uh, what information do you need?",
        ),
        (
            "customspeech3",
            "Right, listen here ya drongo, I've had it up to me back teeth with these flamin' galahs carryin' on like pork chops. "
            "Fair dinkum, if one more bludger tries to pull the wool over me eyes, I'll be flat out like a lizard drinkin', "
            "sortin' out this dog's breakfast. Me old cobber Bazza reckons these muppets couldn't organize a piss-up in a brewery, "
            "and stone the crows, he's not wrong.",
        ),
        (
            "customspeech4",
            "Righto, so the surf's up and the beach is packed tighter than a tin of sardines. "
            "You’ll find every man and his dog out there havin’ a crack. "
            "If you’re keen for a yarn, just chuck on your thongs and head down the track.",
        ),
        (
            "customspeech5",
            "Listen here ya flamin' galah, I’ve been flat out like a lizard drinkin’ all day tryin’ to fix this mess. "
            "Stone the crows, if one more bloke reckons he can do it better, "
            "I’ll tell him to rack off quicker than a roo in the headlights.",
        ),
        (
            "customspeech6",
            "Good evening folks, and welcome to the show. "
            "Out here in the lucky country, we’re all about mateship, having a yarn, and enjoying a fair go. "
            "Stick around, we’ve got plenty lined up for you tonight.",
        ),
        (
            "customspeech7",
            "Thanks for stoppin' by. Grab yourself a cuppa, make yourself at home, and let's get started. "
            "We’ve got a lot to chat about, from everyday life here in Australia to the things that make this place so unique.",
        ),
        (
            "customspeech8",
            "The other day I was out on a morning walk when I spotted a mob of kangaroos hopping across the paddock. "
            "You don’t see that every day unless you’re out bush, but it’s a reminder of how special this country really is.",
        ),
        (
             "customspeech9",
             "Out in the wilds of the interwebs, there roams a fierce creature known only as DragonScarlet. "
             "Don’t be fooled by her graceful looks - this sheila’s a dead-set apex predator in the world of shooters. "
             "One moment you’re wandering about, thinking you’re safe... next moment? Bam! "
             "You’re flat on your back, tears in your eyes, wonderin’ what just happened. "
             "She stalks noobs like a saltie stalks tourists on the riverbank - silent, patient, and then snap! "
             "It’s game over, mate.",
        ),
    ]
)


# ----------------------------
# Generator
# ----------------------------

def build_rows(voices, prompts):
    """
    Yields rows of:
    outputfilename_without_extension, prompt_transcription, prompt_wav, text_to_synthesize
    """
    for v in voices:
        vname = v["name"]
        transcription = v["prompt_transcription"]
        wav = v["prompt_wav"]

        for pname, synth_text in prompts.items():
            outname = f"{vname}_{pname}"  # no extension
            yield (outname, transcription, wav, synth_text)


def write_tsv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        for r in rows:
            # Join strictly with tabs; no header row
            f.write("\t".join(r) + "\n")


def parse_args():
    p = argparse.ArgumentParser(description="Generate a ZipVoice inference TSV from hardcoded voices and prompts.")
    p.add_argument(
        "-o",
        "--output",
        default="test.tsv",
        help="Output TSV filename (default: test.tsv)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    out_path = Path(args.output)
    rows = list(build_rows(VOICES, PROMPTS))
    write_tsv(out_path, rows)
    print(f"Wrote {len(rows)} rows to {out_path}")


if __name__ == "__main__":
    main()

