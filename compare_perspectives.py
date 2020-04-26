import ast
import time
import pickle
import datetime


perspective_keys = ['TOXICITY', 'SEVERE_TOXICITY', 'INFLAMMATORY', 'PROFANITY', 'INSULT', 'OBSCENE', 'SPAM']
keywords = ['haha', 'joke', 'kek', 'lmao', 'lol', 'lulz', 'rofl']

'''
Streaming Pickle code sourced found here: 
https://code.google.com/archive/p/streaming-pickle/
'''

try:
  from cPickle import dumps, loads
except ImportError:
  from pickle import dumps, loads


def s_dump(iterable_to_pickle, file_obj):
  '''dump contents of an iterable iterable_to_pickle to file_obj, a file
  opened in write mode'''
  for elt in iterable_to_pickle:
    s_dump_elt(elt, file_obj)


def s_dump_elt(elt_to_pickle, file_obj):
  '''dumps one element to file_obj, a file opened in write mode'''


  # original code edited this to cast the return of dumps to a string since dumps() returns bytes object
  pickled_elt_str = str(dumps(elt_to_pickle))

  file_obj.write(pickled_elt_str)
  # record separator is a blank line
  # (since pickled_elt_str might contain its own newlines)
  file_obj.write('\n\n')


def s_load(file_obj):

  '''load contents from file_obj, returning a generator that yields one
  element at a time'''

  cur_elt = []
  for line in file_obj:
    cur_elt.append(line)

    if line == '\n':
      pickled_elt_str = ''.join(cur_elt)

      # original code edited to convert from bytes to str with ast
      elt = loads(ast.literal_eval(pickled_elt_str))

      cur_elt = []
      yield elt


def run():

    '''
    NOTE: Perspectives are a dict with format:
    "perspectives": {"TOXICITY": 0.12395912, "SEVERE_TOXICITY": 0.07129683, "INFLAMMATORY": 0.24159998, "PROFANITY": 0.15595378, "INSULT": 0.090337045, "OBSCENE": 0.4239637, "SPAM": 0.42458713}}
    '''


    # initialize keyword perspective
    # keyword_perspectives.get(key)[0] = num coms with keyword in it
    # keyword_perspectives.get(key)[1] = dict of all the average perspectives for every com with that keyword in it
    keyword_perspectives = {}
    for key in keywords:
        keyword_perspectives[key] = [0, {"TOXICITY": [0], "SEVERE_TOXICITY": [0], "INFLAMMATORY": [0], "PROFANITY": [0], "INSULT": [0], "OBSCENE": [0], "SPAM": [0]}]

    # initialize all_perspectives
    # all_perspectives [0] = num all comments in data set
    # all_perspectives [1] = average persepctives for all comments in data set
    all_perspectives = [0, {"TOXICITY": [0], "SEVERE_TOXICITY": [0], "INFLAMMATORY": [0], "PROFANITY": [0], "INSULT": [0], "OBSCENE": [0], "SPAM": [0]}]

    all_perspectives_batch = []

    num_coms_in_line = 0
    num_coms_out_of_line = 0
    num_errors = 0

    with open('./Data/cleaned_data.txt') as streaming_data:

        data_generator = s_load(streaming_data)

        num_thrupple = 0
        for thrupple in data_generator:

            if num_thrupple%50000==0:
                print(f"On line {num_thrupple} at {datetime.datetime.now()}")

            try:
                line = thrupple[2]
                thread = line.get('posts')
                for post in thread:
                    try:
                        perspectives = post.get('perspectives')
                        # update all_persepctives
                        all_perspectives[0] = all_perspectives[0] + 1
                        all_perspectives_batch.append(perspectives)
                        for pk in perspective_keys:
                            all_perspectives[1].get(pk)[0] += perspectives.get(pk)

                        # find the right cleaned_com for this post:
                        post_num = post.get('no')
                        try:
                            for uid, com in thrupple[1]:
                                if uid == post_num:
                                    num_coms_in_line += 1
                                    # update keyword_perspectives
                                    for key in keywords:
                                        if key in com:
                                            keyword_perspectives.get(key)[0] += 1
                                            for pk in perspective_keys:
                                                keyword_perspectives.get(key)[1].get(pk)[0] += perspectives.get(pk)
                                    num_coms_in_line += 1
                                    break
                        except:
                            num_coms_out_of_line += 1

                        post_num += 1
                    except:
                        num_errors += 1
            except:
                num_errors += 1

            # every thread, we save the batch of perspectives
            file_saving_to = open("./Data/Perspectives/perspectives_for_every_com.txt", "a")
            s_dump(all_perspectives_batch, file_saving_to)
            file_saving_to.close()
            all_perspectives_batch.clear()

            num_thrupple += 1

    # divide out the number of coms to make the average perspective values
    for pk in perspective_keys:
        # all_perspectives[1].get(pk)[0] = all_perspectives[1].get(pk)[0]/all_perspectives[0]
        for key in keywords:
            if keyword_perspectives.get(key)[0] > 0:
                keyword_perspectives.get(key)[1].get(pk)[0] = keyword_perspectives.get(key)[1].get(pk)[0]/keyword_perspectives.get(key)[0]

    # print(f"All perspectives: {all_perspectives}")
    for key in keywords:
        print(f"{key} persepectives: {keyword_perspectives.get(key)}")


    print(f"Num errors: {num_errors}")
    print(f"Num coms in line: {num_coms_in_line}")
    # out of line coms should be those that weren't able to be cleaned and so don't appear in the list of  cleaned coms
    print(f"Num coms out of line: {num_coms_out_of_line}")


    # save all_perspectives and keyword_perspectives
    start_time = time.time()
    print("about to normal pickle dump the all_perspectives data")
    pickle_out = open("./Data/Perspectives/all_perspectives.pkl", "wb")
    desc = f"all_p[0] = num comments in data set. all_p[1] = average perspectives for all the coms. (Made on {datetime.datetime.now()})."
    pickle.dump((desc, all_perspectives), pickle_out)
    pickle_out.close()
    print(f"Succesfully saved all_perspectives  in {time.time() - start_time} seconds\n")

    start_time = time.time()
    print("about to normal pickle dump the keyword_perspectives data")
    pickle_out = open("./Data/Perspectives/keyword_perspectives.pkl", "wb")
    desc = f"keyword_p.get(key)[0] = num comments in data set that have that keyword in it. keyword_p.get(key)[1] = average perspectives for all the coms that have that keyword. (Made on {datetime.datetime.now()})."
    pickle.dump((desc, keyword_perspectives), pickle_out)
    pickle_out.close()
    print(f"Succesfully saved keyword_perspectives in {time.time() - start_time} seconds\n")


if __name__ == "__main__":

    start = time.time()
    print("Program has started\n")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")