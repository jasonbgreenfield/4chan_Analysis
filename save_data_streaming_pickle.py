import datetime
import time
import json
import ast



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

    time_before_list = time.time()

    list_of_data_lines = []

    with open('./Data/pol_062016-112019_labeled.ndjson') as data_file:

        for line in data_file:
            j_content = json.loads(line)
            list_of_data_lines.append(j_content)

            if len(list_of_data_lines)%50000==0:
                print(f"On line {len(list_of_data_lines)} at {datetime.datetime.now()}")

            if len(list_of_data_lines)>90000:
                break

        data_file.close()

    time_after_list = time.time()
    print(f"Made list of data lines in: {time_after_list - time_before_list} seconds")

    # Streaming Pickle save the big list of data lines
    f = open("../data/streaming_pickle_data.txt", "w")
    s_dump(list_of_data_lines, f)
    f.close()

    print(f"Streaming Pickle Saved the data in {time.time() - time_after_list} seconds")

    return

if __name__ == "__main__":

    start = time.time()
    print("Program has started\n")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")