from ib_insync import IB, Stock, util
import nest_asyncio
nest_asyncio.apply()
from pymongo import MongoClient

# Connect to MongoDB
CONNECTION_STRING = 'mongodb+srv://talalmos:97VsovNUWwy7YuXe@shortsqueeze.rymraqj.mongodb.net/?retryWrites=true&w=majority&appName=ShortSqueeze'
DATABASE_NAME = 'IBKR_db'
COLLECTION_NAME = 'ohlc_data_1'

client = MongoClient(CONNECTION_STRING)
db = client[DATABASE_NAME]
collection = db[COLLECTION_NAME]

# Connect to IB
ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)  # Adjust the host, port, and clientId as needed

# Fetch the positions from your account
positions = ib.reqPositions()
tickers = [position.contract.symbol for position in positions if isinstance(position.contract, Stock)]

# Define the duration and bar size for the data
duration = '1 D'  # 1 day
barSize = '1 hour'  # 1-hour bars

# Loop through each ticker and request historical data
for ticker in tickers:
    contract = Stock(ticker, 'SMART', 'USD')
    bars = ib.reqHistoricalData(contract, endDateTime='', durationStr=duration, barSizeSetting=barSize, whatToShow='TRADES', useRTH=True)
    
    # Convert the data to a DataFrame
    df = util.df(bars)
    
    # Add the ticker name to each row in the DataFrame
    df['ticker'] = ticker
    
    # Convert the DataFrame to a dictionary and insert it into MongoDB
    ohlc_dict = df.to_dict("records")
    collection.insert_many(ohlc_dict)

# Disconnect from IB
ib.disconnect()
