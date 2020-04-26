import ast
import time
import pickle
import datetime
from datetime import date
from collections import Counter


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


def find_all_unique_words_smarter(data):

    cntr_raw = Counter()
    cntr_by_com = Counter()

    counter = 0

    for thrupple in data:
        if counter%50000==0:
            print(f"On thread {counter} at {datetime.datetime.now()}")
        cleaned_coms = thrupple[1]
        for uid, com in cleaned_coms:
            com_set = set(com)
            for word in com:
                cntr_raw[word] += 1
            for word in com_set:
                cntr_by_com[word] += 1
        counter += 1

    print(f"cntr_raw 500 most common out of {len(cntr_raw)} total: {cntr_raw.most_common(500)}")
    print(f"cntr_by_com 500 most common our of {len(cntr_by_com)} total: {cntr_by_com.most_common(500)}")

    return cntr_raw, cntr_by_com


def run():

    # Streaming Pickle load the big list of data lines
    with open("./Data/cleaned_data.txt", "r") as f:
        data_generator = s_load(f)

        # 1. make the counters
        unique_words, unique_words_by_com = find_all_unique_words_smarter(data_generator)


        # 2. then save the counters

        # we'll pickle save the list of unique words normally, no streaming pickle because it should be small enough
        start_time = time.time()
        print("about to normal pickle dump data")
        pickle_out = open("./Data/Cooccurence/word_frequency.pkl", "wb")
        desc1 = f"This is a counter object with all the unique words in the data set and their counts. (Made on {date.today()})."
        pickle.dump((desc1, unique_words), pickle_out)
        pickle_out.close()
        print(f"Succesfully saved all unique words in data set in {time.time() - start_time} seconds\n")

        # we'll pickle save the list of unique words normally, no streaming pickle because it should be small enough
        start_time = time.time()
        print("about to normal pickle dump data")
        pickle_out = open("./Data/Cooccurence/word_frequency_by_com.pkl", "wb")
        desc2 = f"This is a counter object with all the unique words (by com, so only one count per com that word is in) in the data set and their counts. HOWEVER, it only counts each word once for every comment it is in. So the comment was turned into a set before put into the Counter(). (Made on {date.today()})."
        pickle.dump((desc2, unique_words_by_com), pickle_out)
        pickle_out.close()
        print(f"Succesfully saved all unique words BY COM in data set in {time.time() - start_time} seconds\n")

    return

if __name__ == "__main__":

    # this file divides the 106GB data set and divides (decimates) it into 10 roughly equal portions

    start = time.time()
    print("Program has started\n")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")