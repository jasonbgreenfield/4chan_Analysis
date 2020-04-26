import ast
import time
import pickle
import datetime
from collections import Counter


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


def pickle_save(all_e, keyword_e):

    start_time = time.time()
    pickle_out = open("./Data/Entities/all_entities.pkl", "wb")
    desc = f"This is a Counter() object where each entry is the text for an entity in the data. This is for the entire data set. (Made on {datetime.datetime.now()})."
    pickle.dump((desc, all_e), pickle_out)
    pickle_out.close()
    print(f"Succesfully saved Counter for all entities in data set in {time.time() - start_time} seconds\n")

    for key in keywords:
        to_save = keyword_e.get(key)
        start_time = time.time()
        pickle_out = open(f"./Data/Entities/{key}_entities.pkl", "wb")
        desc = f"(keyword={key}) This is a Counter() object where each entry is the text for an entity in the data. This is only for comments that contain the keyword {key}. (Made on {datetime.datetime.now()})."
        pickle.dump((desc, to_save), pickle_out)
        pickle_out.close()
        print(f"Succesfully saved Counter for all {key} entities in {time.time() - start_time} seconds\n")


def run():

    # initialize keyword_entities as a Counter() obj
    keyword_entities = {}
    for key in keywords:
        keyword_entities[key] = Counter()

    # all_entities is a Counter() obj for each named entity in the data set
    all_entities = Counter()

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
                        entities = post.get('entities')
                        for dict in entities:
                            entity = dict.get('entity_text')
                            label = dict.get('entity_label')
                            # update all_entities
                            all_entities[entity] += 1

                        # find the right cleaned_com for this post:
                        post_num = post.get('no')
                        try:
                            for uid, com in thrupple[1]:
                                if uid == post_num:
                                    num_coms_in_line += 1
                                    # update keyword_perspectives
                                    for key in keywords:
                                        if key in com:
                                            for dict in entities:
                                                entity = dict.get('entity_text')
                                                label = dict.get('entity_label')
                                                # update all_entities
                                                keyword_entities.get(key)[entity] += 1
                                    num_coms_in_line += 1
                                    break
                        except:
                            num_coms_out_of_line += 1

                        post_num += 1
                    except:
                        num_errors += 1
            except:
                num_errors += 1

            num_thrupple += 1

    to_print = 50
    print(f"For all, {to_print} most common entities: {all_entities.most_common(to_print)}")
    for key in keywords:
        print(f"For {key}, {to_print} most common entities: {keyword_entities.get(key).most_common(to_print)}")

    print(f"Num errors: {num_errors}")
    print(f"Num coms in line: {num_coms_in_line}")
    # out of line coms should be those that weren't able to be cleaned and so don't appear in the list of  cleaned coms
    print(f"Num coms out of line: {num_coms_out_of_line}")

    # now save all the Counter objects
    pickle_save(all_entities, keyword_entities)


if __name__ == "__main__":

    start = time.time()
    print("Program has started\n")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")