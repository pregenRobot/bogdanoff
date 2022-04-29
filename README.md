
# Instructions


## Running

### Binance Public Data Crawler

**Note: Python >=3.8**

Recommended:
```shell

brew install miniconda
conda create --name <env> --file requirements.txt
conda activate --name <env>
cd src/collectors
python binance.py
```

Alternative if you can't be bothered to setup anaconda environment manager
(highly recommended btw)


```
pip install -r requirements.txt
cd src/collectors
python binance.py
```


## Browsing

### Binance Public Data Crawler Results

Use an SQLite Viewer to view all the fetched url paths.

Recommended:
https://sqlitebrowser.org/dl/



