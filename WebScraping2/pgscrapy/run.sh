#!/bin/bash

source /Users/jianhuang/opt/anaconda3/etc/profile.d/conda.sh

conda activate Data35

export PYTHONPATH=/Users/jianhuang/opt/anaconda3/envs/Data35/Data26:/Users/jianhuang/opt/anaconda3/envs/Data35/Data26/WebScraping2:/Users/jianhuang/opt/anaconda3/envs/Data35/Data26/WebScraping2/pgscrapy

python --version 
python /Users/jianhuang/opt/anaconda3/envs/Data35/Data26/WebScraping2/pgscrapy/pgspiderrun.py
