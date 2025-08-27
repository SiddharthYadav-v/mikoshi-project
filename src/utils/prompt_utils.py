"""
Utility functions for prompt cleaning and normalization.

This module provides functions to clean and normalize prompts before sending them to LLMs.
"""

import re
from typing import Pattern


def clean_prompt(prompt: str, 
                 remove_backslashes: bool = True,
                 collapse_whitespace: bool = True,
                 preserve_newlines: bool = False) -> str:
    """
    Normalize a raw prompt string for an LLM:
      - Optionally strip literal backslashes (\\)
      - Replace tabs with spaces
      - Optionally collapse runs of whitespace into a single space
      - Optionally preserve or flatten newlines
    
    Args:
        prompt:           The raw prompt text.
        remove_backslashes: If True, removes all '\\' characters.
        collapse_whitespace: If True, turns any run of spaces/newlines/tabs into one space.
        preserve_newlines:   If True, keeps '\n' (but still collapses tabs/spaces around them).
    
    Returns:
        A cleaned prompt string.
    """
    if not prompt:
        return ""
    
    text = prompt

    if remove_backslashes:
        text = text.replace('\\', '')

    # normalize tabs to spaces
    text = text.replace('\t', ' ')
    
    if collapse_whitespace:
        if preserve_newlines:
            # temporarily mark newlines, collapse other whitespace, restore newlines
            text = text.replace('\n', '<NEWLINE>')
            text = re.sub(r'\s+', ' ', text)
            text = text.replace('<NEWLINE>', '\n')
        else:
            # collapse everything (spaces, newlines, tabs) to single spaces
            text = re.sub(r'\s+', ' ', text)
    
    return text.strip() 