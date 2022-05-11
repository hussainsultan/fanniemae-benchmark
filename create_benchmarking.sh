#!/bin/bash

conda create --name benchmarking

conda activate benchmarking

while read requirement; do mamba install $requirement --yes -c conda-forge; done < requirements.txt
