import re
from typing import List

def extract_markers(markers: str, only_label: bool = False) -> List[str]:
    """
    Extracts marker labels in the formats <PREFIX><SEQ>[(NO)] from a string.
    If only_label is True, returns just <PREFIX><SEQ> (e.g., 'X16' from 'X16(1)').
    Otherwise, returns the full marker including any (NO) (e.g., 'X16(1)').
    Handles both unformatted and formatted marker strings, including comma-separated lists.
    """
    # Match <PREFIX><SEQ> with optional (NO)
    pattern = r'([A-Za-z]+\d+)(\(\d+\))?'
    matches = re.findall(pattern, markers)
    if only_label:
        return [prefix for prefix, _ in matches]
    else:
        return [prefix + suffix for prefix, suffix in matches]

def main():
    examples = [
        ('LLL  X16(1)_ACID_3/8"_LOK', "Unformatted single"),
        ('LLL  A7_ACID_3/8"_LOK,LLL  X16(1)_ACID_3/8"_VCR Male', "Unformatted multiple"),
        ('X16', "Formatted single"),
        ('X14,X15,X16,X17,A5(1),A6', "Formatted multiple"),
    ]

    for s, desc in examples:
        print(f"--- {desc} ---")
        print(f"Original: {s}")
        print("Extracted (full):", extract_markers(s))
        print("Extracted (only_label):", extract_markers(s, only_label=True))
        print()

if __name__ == "__main__":
    main()
