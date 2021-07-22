# Data Mining 2021
This repository contains all the material regarding the final project for the course of Data Mining, for the year 2020/2021.
The required task is to perform an analysis on a dataset of COVID-related tweets. More details in the report.

Ivan Martini #207597

### Setup

0) **Prerequisites**: Have `python` installed. Nothing more should be required on any operating system. I had the chance to test the code only on Archlinux, Ubuntu 20.04 and Windows, but everything went fine.

1) Clone the repository
```
git clone https://github.com/iron512/DataMining2021.git
```

2) Install the requisites. In order to avoid any conflict and keep your system clean, a virtual environment is suggested.
```
python3 -m venv virtual
. virtual/bin/activate
pip install -r requirements.txt
deactivate
```

3) Run the code
In the root folder of the project run:
```
python3 src/cleaner.py data/covid19_tweets 1
```
This command will produce the clean dataset (which is already present)
- To run the code:
```
python3 src/miner.py data/clean_dataset.csv 1 0
```
- To run the baseline:
```
python3 src/baseline.py data/clean_dataset.csv 1
```

All the results are stored in the `data/output/` folder.