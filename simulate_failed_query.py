def simulate_failed_query(lst=[i for i in range(10)], 
                          max_retries=5, 
                          fail=True, 
                          idx_unsuccessful=list()) :
    """Simulate a failed query and handle raised exceptions
    
    Input
        lst: list with integers
        max_retries: number of maximum retries
        fail: condition needed to raise errors
    
    Return
        Inidces where max retries were reached
    """
    for i in lst:
        for attempt in range(max_retries):        
            try:
                print("i: {} \tattempt: {}".format(i, attempt))
                
                # create errors at 2 and 5
                if i == 2 and fail:
                    # reset fail condition on 3. attempt
                    if attempt == 3 and fail:
                        fail = False
                    raise
                    
                # reset fail condition
                if i == 3:
                    fail = True
                    
                # raise error at 5 without reset, reaching max attempts
                if i == 5 and fail:
                    fail = false
                    raise
                    
            except:
                print("\n>>>EXCEPTION AT {} --> RETRY {}. time\n".format(i, attempt))
                
            else:
                print("\tAttempt successful")
                break

        else:
            print("i: {} MAX RETRIES reached. Append to idx_unsuccessful\n".format(i, max_retries))
            # store idx at which max_retries was reached
            idx_unsuccessful.append(i)
            
    return idx_unsuccessful
            
simulate_failed_query()