import time
import nltk
import pickle
import datetime
import ast
from collections import Counter

keywords = ['haha', 'joke', 'lmao', 'lol', 'lulz', 'rofl']
keyword_counters = {}


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


def get_list_of_bigrams_in_thread(coms_list, counter):
    '''

    :param coms_list: list of tuples (uid, cleaned_com)
    :param counter: index in the file cleaned_data.txt so we can print our progression throughout
    :return: returns one list of al the bigrams in all the comments listed in the coms list
    '''

    if counter%50000==0:
        print(f"On thrupple {counter} at {datetime.datetime.now()}")

    to_return = []

    for uid, com in coms_list:
        to_return.extend(list(nltk.bigrams(com)))

    return to_return



def make_keyword_counters():
    """
    This is all the bigrams in all the comments. So each pair of two words that appear next to each other.
    :param comments:
    :return:
    """

    with open("./Data/cleaned_data.txt", "r") as f:

        data_generator = s_load(f)

        '''
        # This methis takes too much memory and time to do, but it works on smaller data sets 
        # num_thrupples = 5000000 # this is exagerated just to make sure we don't cut off the zip() too early
        # terms_bigram = [get_list_of_bigrams_in_thread(thrupples[1], counter) for thrupples, counter in zip(data_generator, range(num_thrupples))]
        '''

        num_thrupple = 0
        for thrupple in data_generator:

            if num_thrupple % 50000 == 0:
                print(f"On thrupple {num_thrupple} at {datetime.datetime.now()}")
            for uid, com in thrupple[1]:

                for key in keywords:
                    if key in com:
                        for i in range(len(com)):
                            if com[i] == key:
                                try:
                                    keyword_counters.get(key)[(com[i-1], com[i])] += 1
                                except:
                                    pass
                                try:
                                    keyword_counters.get(key)[(com[i], com[i+1])] += 1
                                except:
                                    pass

            num_thrupple += 1
    for key in keywords:
        print(f"20 most common bigrams for {key}: {keyword_counters.get(key).most_common(20)}")

    return


def save_keyword_counters():

    for key in keywords:

        pickle_out = open(f"./Data/BiGrams/{key}_bigrams.pkl", "wb")
        desc = f"Counter obj with all the bigrams containing {key}. (Made on {datetime.datetime.now()})."
        pickle.dump((desc, keyword_counters.get(key)), pickle_out)
        pickle_out.close()

    return


def init_keyword_counters():

    for key in keywords:
        keyword_counters[key] = Counter()

    return


def run():

    init_keyword_counters()

    make_keyword_counters()

    save_keyword_counters()

    return


if __name__ == "__main__":
    start = time.time()
    print("Program has started")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")