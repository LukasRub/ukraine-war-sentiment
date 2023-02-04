import re
import sys
import json
import pickle
import argparse
from pathlib import Path
from collections import OrderedDict

import inquirer


FLAIR_INPUT_PATH = "data/flairs/raw/flairs_aggregated.pkl"
FLAIR_OUTPUT_PATH = "data/flairs/processed/flairs_tagged.jsonl"
LABELLING_KEYS = ("key", "value", "needs_review")
REGEX_PATTERN = "(?<=\()(.+)(?=\:)"


def save_answer(answer, dest_path):
    with open(dest_path, "a", encoding="utf8") as fp:
        fp.write(json.dumps(answer, ensure_ascii=False))
        fp.write("\n")


def resolve_answer(flair_id, prompt_selection):
    line_template = OrderedDict.fromkeys(LABELLING_KEYS, value=False)
    prompt_selection = prompt_selection[LABELLING_KEYS[1]] # Unpack the dict
    if prompt_selection:
        value = re.search(REGEX_PATTERN, prompt_selection).group(1)
    else:
        value = None

    line_template[LABELLING_KEYS[0]] = flair_id
    line_template[LABELLING_KEYS[1]] = value
    return line_template


def prompt_options(key, values):
    choices = (["({}: {})".format(k,v) for k, v 
               in sorted(values.items(), key=lambda x: x[1], reverse=True)] 
               + [None])
    question = inquirer.List(LABELLING_KEYS[1], message=key, choices=choices)
    return inquirer.prompt([question], raise_keyboard_interrupt=True)


def label_keys(flair_data, dest_path, continue_labelling):

    if not continue_labelling:
        # Empty the result file if the flag is set to False
        dest_path.open("w").close()
    
    needs_review = False

    for key, values in flair_data.items():
        try:
            prompt_selection = prompt_options(key, values)
        except KeyboardInterrupt:
            # Exit the loop and the program if keyboard interrupt is detected
            sys.exit(0)
        else:
            answer = resolve_answer(key, prompt_selection)
            save_answer(answer, dest_path)


def main(flair_data, dest_path, continue_labelling):

    if continue_labelling and dest_path.exists() and dest_path.is_file():
        # If selected to continue labelling, find all keys that were already
        # labelled in the destination file
        labelled_keys = list()
        with open(dest_path, "r") as fp:
            for line in fp:
                labelled_keys.append(json.loads(line)[LABELLING_KEYS[0]])

        # Filter flair data for keys to be labelled
        flair_data = {key:value for key, value in flair_data.items()
                      if key not in labelled_keys}
    else:
        dest_path.parent.mkdir(exist_ok = True)

    label_keys(flair_data, dest_path, continue_labelling)


def unpickle_source_file(path_str):
    try:
        return pickle.load(Path(path_str).open("rb"))
    except (OSError, pickle.UnpicklingError) as e:
        error_message = "Error while opening the file: {}".format(e)
        raise argparse.ArgumentTypeError(error_message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="""Manually annotate flair data by selecting appropriate
                       options from deserialized flair data file"""
    )

    parser.add_argument("--source_path", required=False, 
        default=FLAIR_INPUT_PATH, type=unpickle_source_file,
        help="""Source file for serialized flair data""")
    
    parser.add_argument("--dest_path", required=False, 
        default=Path(FLAIR_OUTPUT_PATH), type=Path,
        help="""Source file for serialized flair data""")

    parser.add_argument("--continue", action="store_true", 
        dest="continue_labelling",
        help="""Prompt flairs to be labelled that were not labelled previously
                in the destination file.""")

    args = parser.parse_args()

    main(flair_data=args.source_path, dest_path=args.dest_path,
         continue_labelling=args.continue_labelling)
