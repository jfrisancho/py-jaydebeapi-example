#!/bin/bash

# Run analysis and check result
if python main.py --approach RANDOM --coverage-target 0.2 --unattended; then
    echo "Analysis successful"
    # Continue with next steps
else
    echo "Analysis failed"
    exit 1
fi