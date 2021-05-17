from kiteconnect import KiteConnect, KiteTicker
import pandas as pd
import datetime
from itertools import cycle, islice
import re

# Exhaustive list of all the stocks for which data is needed
STOCK_LIST = \
["NIFTY", "BANKNIFTY", "FINNIFTY","AARTIIND","ACC","ADANIENT"
,"ADANIPORTS","ALKEM","AMARAJABAT","AMBUJACEM","APLLTD","APOLLOHOSP"
,"APOLLOTYRE","ASHOKLEY","ASIANPAINT","AUBANK","AUROPHARMA"
,"AXISBANK","BAJAJ-AUTO","BAJAJFINSV","BAJFINANCE","BALKRISIND","BANDHANBNK"
,"BANKBARODA","BATAINDIA","BEL","BERGEPAINT","BHARATFORG","BHARTIARTL"
,"BHEL","BIOCON","BOSCHLTD","BPCL","BRITANNIA","CADILAHC","CANBK","CHOLAFIN"
,"CIPLA","COALINDIA","COFORGE","COLPAL","CONCOR","CUB","CUMMINSIND","DABUR"
,"DEEPAKNTR","DIVISLAB","DLF","DRREDDY","EICHERMOT","ESCORTS","EXIDEIND"
,"FEDERALBNK","FINNIFTY","GAIL","GLENMARK","GMRINFRA","GODREJCP","GODREJPROP"
,"GRANULES","GRASIM","GUJGASLTD","HAVELLS","HCLTECH","HDFC","HDFCAMC"
,"HDFCBANK", "HDFCLIFE","HEROMOTOCO","HINDALCO","HINDPETRO","HINDUNILVR"
,"IBULHSGFIN","ICICIBANK","ICICIGI","ICICIPRULI","IDEA","IDFCFIRSTB","IGL"
,"INDIGO","INDUSINDBK","INDUSTOWER","INFY","IOC","IRCTC","ITC","JINDALSTEL"
,"JSWSTEEL","JUBLFOOD","KOTAKBANK","L&TFH","LALPATHLAB","LICHSGFIN","LT"
,"LTI","LTTS","LUPIN","M&M","M&MFIN","MANAPPURAM","MARICO","MARUTI"
,"MCDOWELL-N","MFSL","MGL","MINDTREE","MOTHERSUMI","MPHASIS","MRF","MUTHOOTFIN"
,"NAM-INDIA","NATIONALUM","NAUKRI","NAVINFLUOR","NESTLEIND","NMDC","NTPC"
,"ONGC","PAGEIND","PEL","PETRONET","PFC","PFIZER","PIDILITIND","PIIND","PNB"
,"POWERGRID","PVR","RAMCOCEM","RBLBANK","RECLTD","RELIANCE","SAIL","SBILIFE"
,"SBIN","SHREECEM","SIEMENS","SRF","SRTRANSFIN","SUNPHARMA","SUNTV","TATACHEM"
,"TATACONSUM","TATAMOTORS","TATAPOWER","TATASTEEL","TCS","TECHM","TITAN"
,"TORNTPHARM","TORNTPOWER","TRENT","TVSMOTOR","UBL","ULTRACEMCO","UPL","VEDL"
,"VOLTAS","WIPRO","ZEEL"]

indices = ["NIFTY", "BANKNIFTY", "FINNIFTY"]

MONTH_LOOKUP = {'01':'JAN', '02':'FEB', '03':'MAR', '04':'APR', '05':'MAY',
                '06':'JUN', '07':'JUL', '08':'AUG', '09':'SEP', '10':'OCT',
                '11':'NOV', '12':'DEC'}

access_token = '###############' # changes every day
key_secret = '********************'

# Set up Kite
kite = KiteConnect(api_key=key_secret, access_token = access_token)

# get list of tradeable NFO instruments
instrument_df = pd.DataFrame(kite.instruments(exchange=kite.EXCHANGE_NFO))


def get_months_of_interest_short_name(start_month_number):
    '''
        This calculates the months of interest as in:current month, next month
        and next to next month,while maintaing the circular nature of order of
        months, i.e. DEC is again followed by JAN.

        Parameter:
        ------------------
        start_month_number: It is the current month Number represented as a zero
                            padded string. Ex: '05', '12'

        Returns:
        ----------------
        Short month names of three consecutive months starting from current
        month. Ex: MAY, JUN, JUL
    '''

    global MONTH_LOOKUP

    pool = cycle(list(MONTH_LOOKUP))

    # skip till last month and start at current month
    start_at_this_month = islice(pool, int(start_month_number)-1, None)

    # return the three months needed
    return [MONTH_LOOKUP[str(next(start_at_this_month))]
           ,MONTH_LOOKUP[str(next(start_at_this_month))]
           ,MONTH_LOOKUP[str(next(start_at_this_month))]
        ]


def get_fut_tradingsymbols(months_of_interest, current_year):
    '''
        retruns a list of unique fut instrumnets
        Format:
            NIFTY21JULFUT, NIFTY21JUNFUT, NIFTY21MAYFUT --> NAME_YY_MMM_FUT

        Parameters:
        ---------------------
        current_year: Year in 'YY' format. Can either be a zero padded string
                      or integer

        months_of_interest: Short name of Interested months. Expectation is to
                            have legth of 3 for current, next and next to next
                            months.
        Returns:
        ------------------
        list containing tradingsymbol of all the required FUT instruments
    '''
    global instrument_df

    # name of the stocks
    names = instrument_df['name'].unique()

    # unique trading symbols of all indices
    stock_symbols = instrument_df['tradingsymbol'].unique()

    # this will hold all the tradingsymbols
    fut_symbols = []

    # Check if months rollover to next year ex: ['NOV', 'DEC', 'JAN']
    # this check will happen only once every day and logic of other downstream
    # processes is dependent on it
    if (months_of_interest[2].upper() == "JAN") \
        or (months_of_interest[1].upper() == "JAN"): # Months rollover to next year

        # which month is JAN, 2nd or 3rd?
        jan_index = months_of_interest.index("JAN")
        
        if jan_index == 1: #["DEC", "JAN", "FEB"]
            this_year_month = "DEC"
            next_year_months = ["JAN", "FEB"]

            for name in names:
                # below expression looks for combination of current year &
                # this years months and Next Yera & Next Years months
                expression = (fr"\b({name})(({current_year}{this_year_month})" # this year and month
                              fr"|({int(current_year)+1}(({next_year_months[0]})" # next year and months
                              fr"|({next_year_months[1]}))))(FUT)\b") # next year and months

                compiled = re.compile(expression)
                fut_symbols.extend(list(filter(compiled.match, stock_symbols)))

        else: # it can only be 2 if not 1  --> ["NOV", "DEC", "JAN"]
            this_year_months = ["NOV", "DEC"]
            next_year_month = "JAN"

            for name in names:
                # below expression looks for combination of current year &
                # this years months and Next Yera & Next Years months
                expression = (fr"\b({name})(({current_year}"
                        fr"(({this_year_months[0]})|({this_year_months[1]})))"
                        fr"|({int(current_year)+1}({next_year_month})))(FUT)\b"
                )

                compiled = re.compile(expression)
                fut_symbols.extend(list(filter(compiled.match, stock_symbols)))

    else: # All the Months are from same year and no rollover is required
        for name in names:
            expression = (fr"\b({name})({current_year})({months_of_interest[0]}"
                fr"|{months_of_interest[1]}|{months_of_interest[2]})(FUT)\b")
            compiled = re.compile(expression)
            fut_symbols.extend(list(filter(compiled.match, stock_symbols)))

    return fut_symbols


def get_opt_weekly_tradingsymbols(current_month, current_year):
    '''
        From the list of unique symbols for the indices select those symbols
        that matches the specifed pattern. Pattern is to get the symbols of
        weekly indices from this month only.

        returns a list of weekly  Options trading symbols.
        Format: NIFTY2151214100CE --> NAME_YY_M_DD_STRIKE_CE/PE

        For Options these are monthly and weekly expiries respectively.
        Only indices have weekly expiries(contracts).

        It is the responsibility of developer/user to provide right indices.

        Parameter:
        ------------------
        current_month: Number representing current month. This will be in string
                       format and zero padded for single numerals. Ex: '05'-->5,
                       '12'-->12.
                       Will also work if int is provided like 5 for MAY.

        Returns:
        ---------------------
        A list of weekly trading symbols for Options of all the indices.
    '''
    global instrument_df, indices

    # unique trading symbols of all indices
    index_symbols = instrument_df[instrument_df['name'].isin(indices)]\
                    ['tradingsymbol'].unique()

    # this list will contain only weekly symbols of this month
    # from all the symbols
    weekly_symbols = []

    # fetch the needed symbols for all the indices and populate the master list
    for index in indices:
        expression = fr"\b{index}{int(current_year)}{int(current_month)}[\d]{{2}}\d+((CE)|(PE))\b"
        compiled = re.compile(expression)
        weekly_symbols.extend(list(filter(compiled.match, index_symbols)))

    return weekly_symbols


def get_opt_monthly_tradingsymbols(months_of_interest, current_year):

    '''
        From a list of symbols for the specifed Instruments(Stocks), it picks
        those tradingsymbols that are from current month, next month and next to
        next month Only.
        It must take care of cases when the 2nd and/or 3rd months are from next
        calendar year. For example NOV, DEC, JAN or DEC, JAN, FEB. This can be
        stated as if JAN is in either 2nd or 3rd position in the array, then we
        have to handle the case differently.

        returns a list of monthly Options trading symbols.

        Format:
            NIFTY21MAY12850CE --> NAME_YY_MMM_STRIKE_CE/PE

        Parameters:
        ----------------------
        months_of_interest: list of strings containing short name of months and
                            of length 3. Should be ALL CAPS.
        current_year: Present year. Can either be string or int in 'YY' format
    '''
    global instrument_df, indices

    # Name of all the stocks being traded that we are interested in
    # Same could also be fetched from STOCK_LIST, but once that is used up to
    # fetch live instrumnets data for the day, this data should be used
    names = instrument_df['name'].unique()

    # unique trading symbols of all indices
    stock_symbols = instrument_df['tradingsymbol'].unique()

    # lsit to hold all the desired symbols
    monthly_symbols = []

    # Symbol calculation
    # check if need rollover to next year
    if (months_of_interest[2].upper() == "JAN") \
        or (months_of_interest[1].upper() == "JAN"): # rollover needed
        # NAME_YY_MMM_STRIKE_CE/PE
        jan_index = months_of_interest.index("JAN")

        for i in range(0, 3): # three months only
            if i<jan_index:
                for name in names:
                    month = months_of_interest[i]
                    expression = fr"\b{name}{int(current_year)}{month}\d+((CE)|(PE))\b"
                    compiled = re.compile(expression)
                    monthly_symbols.extend(list(filter(compiled.match, stock_symbols)))
            else:
                current_year = int(current_year)+1
                for name in names:
                    month = months_of_interest[i]
                    expression = fr"\b{name}{int(current_year)}{month}\d+((CE)|(PE))\b"
                    compiled = re.compile(expression)
                    monthly_symbols.extend(list(filter(compiled.match, stock_symbols)))

    else: # rollover not needed - common case
        for name in names:
            for month in months_of_interest:
                expression = fr"\b{name}{int(current_year)}{month}\d+((CE)|(PE))\b"
                compiled = re.compile(expression)
                monthly_symbols.extend(list(filter(compiled.match, stock_symbols)))

    return monthly_symbols


def get_instrument_tokens():

    '''
        Fetches data
        
        Returns:
        -----------------
        A list of required instrument_tokens for both FUT and OPT combined.
    '''
    today = datetime.datetime.today()
    current_year = today.strftime("%y")
    current_month = today.strftime("%m")
    months_of_interest = get_months_of_interest_short_name(current_month)

    instrument_tokens = []

    # fetch FUT tokens
    instrument_tokens.extend(get_fut_tradingsymbols(months_of_interest, current_year))
    # fetch OPT weekly instruments
    instrument_tokens.extend(get_opt_weekly_tradingsymbols(current_month, current_year))
    # fetch OPT monthly instruments
    instrument_tokens.extend(get_opt_monthly_tradingsymbols(months_of_interest, current_year))

    return instrument_tokens
