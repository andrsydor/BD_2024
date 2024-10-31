# Stack exchange trends big data analysis

### Getting started:
- clone project `git clone https://github.com/andrsydor/BD_2024.git`
- navigate `cd BD_2024`
- create `/data` directory
- load `.csv` files from Stack exchange data explorer (details in Data) into `/data` directory
- configure virtual environment
- install requirements `pip install -r requirements.txt`


### Running ETL:
- `python3 etl.py`


### Data:
We use Posts, Comments, Tags and TagSynonyms data from Stack exchange data explorer `https://data.stackexchange.com/`.
You can simply download corresponding `.csv` files from `select` query
