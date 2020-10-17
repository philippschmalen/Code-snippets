import pandas as pd
import numpy as np
import helper_functions as h #TODO: replace with workbench package
from datetime import datetime
from pytrends.request import TrendReq


def handle_query_results(df_query_result, keywords, query_return_length=261):
	"""Process query results: 
	        (i) check for empty response --> create df with 0s if empty
	        (ii) drop isPartial rows and column
	        (iii) transpose dataframe to wide format (keywords//search interest)

	Input
	    df: dataframe containing query result (could be empty)
	    filename: name of temporary file
	    query_return_length: 261 is normal return length of query result 
	    
	Return
	    Dataframe: contains query results in long format 
	    (rows: keywords, columns: search interest over time)
	"""
	# non-empty df
	if df_query_result.shape[0] != 0:
		# reset_index to preserve date information, drop isPartial column
		df_query_result_processed = df_query_result.reset_index()\
			.drop(['isPartial'], axis=1)

		df_query_result_long = pd.melt(df_query_result_processed, id_vars=['date'], var_name='keyword', value_name='search_interest')

		# long format (date, keyword, search interest)
		return df_query_result_long

	# no search result for any keyword: empty df
	else:        
		# create empty df with 0s
		df_zeros = pd.DataFrame(np.zeros((query_return_length*len(keywords), 3)), columns=['date','keyword', 'search_interest'])
		# replace 0s with keywords
		df_zeros['keyword'] = np.repeat(keywords, query_return_length)

		return df_zeros



def query_googletrends(keywords):
	"""Forward keywords to Google Trends API and process results into long format

	Args
		keywords: list of keywords, with maximum length 5

	Return
		DataFrame with search interest per keyword, preprocessed by handle_query_results()

	"""
	# initialize pytrends
	pt = TrendReq()

	# pass keywords to api
	pt.build_payload(kw_list=keywords) 

	# retrieve query results: search interest over time
	df_query_result_raw = pt.interest_over_time()

	# preprocess query results
	df_query_result_processed = handle_query_results(df_query_result_raw, kw_batch)

	return df_query_result_processed



from time import sleep
from random import randint # for random timeout +/- 5

def query(keywords, filepath, filename, max_retries=1, idx_unsuccessful=list(), timeout=20) :
    """Handle failed query and handle raised exceptions
    
    Input
        keywords: list with keywords for which to retrieve news
        max_retries: number of maximum retries
        until_page: maximum number of retrievd news page
        
    
    Return
        Inidces where max retries were reached
    """    
    # retry until max_retries reached
    for attempt in range(max_retries):   

        # random int from range around timeout 
        timeout_randomized = randint(timeout-3,timeout+3)

        try:
            df_result = query_googletrends(keywords)


        # handle query error
        except Exception as e:

            # increase timeout
            timeout += 5

            print(">>> EXCEPTION at {}: {} \n Set timeout to {}\n".format(i, e, timeout))
            # sleep
            h.sleep_countdown(timeout_randomized, print_step=2)


        # query was successful: store results, sleep 
        else:

            # generate timestamp for csv
            stamp = h.timestamp_now()

            # merge news dataframes and export query results
            h.make_csv(df_result, filename, filepath, append=True)

            # sleep
            h.sleep_countdown(timeout_randomized)
            break

    # max_retries reached: store index of unsuccessful query
    else:
        h.make_csv(pd.DataFrame(keywords), "unsuccessful_queries.csv", filepath, append=True)
        print("{} appended to unsuccessful_queries\n".format(keywords))


# timestamp for filenames
stamp = h.timestamp_now()


##########################################
############## TODO: Define parameters
# define filenames
filename_gtrends = stamp+'gtrends.csv'
filename_meta = stamp+'gtrends_metadata.csv'

# set sample size (number of keywords)
sample_size = 3835 # minimum: 5
##########################################


# load keywords as input for API query  
df_query_input = pd.read_csv('../../data/interim/keywords.csv').iloc[:sample_size,:]

# store metadata of query
df_query_input['date_query_googletrends'] = datetime.today().strftime('%Y-%m-%d')
h.make_csv(df_query_input, filename_meta,'../../data/raw', header=True)

print("Query for {} keywords".format(len(df_query_input)))

# create batches of 5 keywords and feed to googletrends query 
for i in range(0,len(df_query_input)-4, 5)[:sample_size]:

	print("{}:{}/{}".format(i,i+5,sample_size))

	# create batches with 5 keywords
	kw_batch = [k for k in df_query_input.keyword[i:i+5]]

	# # feed batch to api query function and store in csv
	query(keywords=kw_batch, filepath='../../data/raw', filename=filename_gtrends)

