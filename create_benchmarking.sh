#!/bin/bash

conda create --name benchmarking

conda activate benchmarking

while read requirement; do mamba install --yes $requirement; done < requirements.txt
