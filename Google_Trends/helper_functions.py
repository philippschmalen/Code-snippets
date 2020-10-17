from time import strftime
from time import sleep
import sys 
import os
import pandas as pd




def sleep_countdown(duration, print_step=1):
    """Sleep for certain duration and print remaining time in steps of print_step

    Input
        duration: duration of timeout (int)
        print_step: steps to print countdown (int)

    Return 
        None
    """
    sys.stdout.write("\r Seconds remaining:")

    for remaining in range(duration, 0, -1):
        # display only steps
        if remaining % print_step == 0:
            sys.stdout.write("\r")
            sys.stdout.write("{:2d}".format(remaining))
            sys.stdout.flush()

        sleep(1)

    sys.stdout.write("\r Complete!\n")

def timestamp_now():
    """Create timestamp string in format: yyyy/mm/dd-hh/mm/ss
        primaryliy used for file naming

    Input
        None

    Return
        String: Timestamp for current time

    """

    timestr = strftime("%Y%m%d-%H%M%S")
    timestamp = '{}'.format(timestr)  

    return timestamp







def make_csv(x, filename, data_dir, append=False, header=False, index=False):
    '''Merges features and labels and converts them into one csv file with labels in the first column

        Input
            x: Data features
            file_name: Name of csv file, ex. 'train.csv'
            data_dir: The directory where files will be saved

        Return
            None: Create csv file as specified
    '''

    # create dir if nonexistent
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # make sure its a df
    x = pd.DataFrame(x)

    # export to csv
    if not append:
        x.to_csv(os.path.join(data_dir, filename), 
                                     header=header, 
                                     index=index)
    # append to existing
    else:
        x.to_csv(os.path.join(data_dir, filename),
                                     mode = 'a',
                                     header=header, 
                                     index=index)        

    # nothing is returned, but a print statement indicates that the function has run
    print('Path created: '+str(data_dir)+'/'+str(filename))
