# DesafioVisie
A script capable of collecting closing prices from Alpha Vantage API and write it on SQLite.

# Dependencies
Tested on Python 3.9+<br>
API Key from Alpha Vantage<br>
`pip install requests`<br>
`pip install pandas`<br>
`pip install sqlalchemy`<br>

# Type
Read and write.

# Getting started
0) Fill `keys.py KEYS["api_key"]` with your Alpha Vantage API key.<br>
[Alpha Vantage API](https://www.alphavantage.co/) <br><br>
1) Perform dependencies installation:<br>
`pip install requests`<br>
`pip install pandas`<br>
`pip install sqlalchemy`<br><br>
2) Then try to perform an extraction:<br>
`py.exe main.py IBM -e`<br><br>
3) You may also want to view the data without consulting the database. If you do so, perform a conversion:<br>
`py.exe main.py IBM -c`

# More information
Author(s): Bruno Lançoni<br>
License: GNU General Public License family<br>
version: 1.0.0

# Utilities
[Alpha Vantage API](https://www.alphavantage.co/) <br>
