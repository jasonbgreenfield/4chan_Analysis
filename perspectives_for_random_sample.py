import ast
import time
import pickle
import datetime
import random


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

'''
This is the end of the Streaming Pickle code'''


def init_k_c():
    keyword_counter = {}
    for key in keywords:
        keyword_counter[key] = {"TOXICITY": [0], "SEVERE_TOXICITY": [0], "INFLAMMATORY": [0], "PROFANITY": [0],
                                "INSULT": [0], "OBSCENE": [0], "SPAM": [0]}
    return keyword_counter


def init_s_p(a_p, k_p):
    sample_perspectives = {}
    for key in keywords:
        # s_p.get(key)[0] = counter for index into sample range
        # s_p.get(key)[1] = counter for how many perspectives we added in the sample size (accounts for empty list perspectives in all_p)
        # s_p.get(key)[2] = sorted list of random ints for the sample set for this keyword
        # s_p.get(key)[3] = the summed perspectives for the sample set
        samples = random.sample(range(a_p[0]), k_p.get(key)[0])
        samples.sort()
        sample_perspectives[key] = [[0], [0], samples,
                                    {"TOXICITY": [0], "SEVERE_TOXICITY": [0], "INFLAMMATORY": [0], "PROFANITY": [0],
                                     "INSULT": [0], "OBSCENE": [0], "SPAM": [0]}]
    return sample_perspectives


def update_s_p(p, s_p, c):
    for key in keywords:
        if c == s_p.get(key)[2][s_p.get(key)[0][0]]:
            try:
                for pk in perspective_keys:
                    s_p.get(key)[3].get(pk)[0] += p.get(pk)
                s_p.get(key)[0][0] += 1
                s_p.get(key)[1][0] += 1
            except:
                continue
    return s_p


def divide_s_p(s_p):
    for pk in perspective_keys:
        for key in keywords:
            if s_p.get(key)[3].get(pk)[0] > 0:
                s_p.get(key)[3].get(pk)[0] = s_p.get(key)[3].get(pk)[0] / s_p.get(key)[1][0]
    return s_p


def update_k_c(k_c, s_p, k_p):
    for key in keywords:
        for pk in perspective_keys:
            # all the perspectives for the kwyrods were greater than the aggregate, except for spam which was lower for all of them
            if key == 'kek':
                if (pk == 'SEVERE_TOXICITY') or (pk == 'OBSCENE') or (pk == 'SPAM'):
                    if s_p.get(key)[3].get(pk)[0] > k_p.get(key)[1].get(pk)[0]:
                        k_c.get(key).get(pk)[0] += 1
                else:
                    if s_p.get(key)[3].get(pk)[0] < k_p.get(key)[1].get(pk)[0]:
                        k_c.get(key).get(pk)[0] += 1
            elif pk != 'SPAM':
                if s_p.get(key)[3].get(pk)[0] > k_p.get(key)[1].get(pk)[0]:
                    k_c.get(key).get(pk)[0] += 1
            else:
                if (key == 'lulz') or (key == 'rofl'):
                    if s_p.get(key)[3].get(pk)[0] > k_p.get(key)[1].get(pk)[0]:
                        k_c.get(key).get(pk)[0] += 1
                else:
                    if s_p.get(key)[3].get(pk)[0] < k_p.get(key)[1].get(pk)[0]:
                        k_c.get(key).get(pk)[0] += 1
    return k_c


def divide_k_c(k_c, num):
    for key in keywords:
        for pk in perspective_keys:
            k_c.get(key).get(pk)[0] = k_c.get(key).get(pk)[0]/num
    return k_c


def print_results(k_c, num):
    for key in keywords:
        print(f"For {key}, with {num} iterations, \n"
              f"Percent for each perspective that is more significant than our keyword value:\n"
              f"{k_c.get(key)}\n")
    return


def sum_perspectives_for_all_iterations(keyword_counter, all_p, keyword_p, num_i):

    for i in range(num_i):

        # initialize aggregate perspectives for each keyword
        sample_perspectives = init_s_p(all_p, keyword_p)

        with open("./Data/Perspectives/perspectives_for_every_com.txt", "r") as f:
            p_generator = s_load(f)
            counter = 0
            for perspectives in p_generator:
                if counter % 44000000==0:
                    print(f"Iteration: {i}, looped up to com: {counter}, at {datetime.datetime.now()}")

                sample_perspectives = update_s_p(perspectives, sample_perspectives, counter)
                counter += 1

        # divide out the number of comments to make the average perspective values
        sample_perspectives = divide_s_p(sample_perspectives)

        # compare perspectives for the sample to what it actually is for the keyword group to increment our counter
        keyword_counter = update_k_c(keyword_counter, sample_perspectives, keyword_p)

    return keyword_counter


def save_results(data, num):

    pickle_out = open("./Data/Perspectives/random_sample_keyword_counter.pkl", "wb")
    desc = f"This is the keyword_counter (dict of Counter() objs) saved after running perspectives_for_random_sample.py. Done with {num} iterations.(Made on {datetime.datetime.now()})."
    pickle.dump((desc, data), pickle_out)
    pickle_out.close()

    return


def run(num_iterations):

    desc_all_p, all_p = pickle.load(open(f"./Data/Perspectives/all_perspectives.pkl", "rb"))
    desc_keyword_p, keyword_p = pickle.load(open(f"./Data/Perspectives/keyword_perspectives.pkl", "rb"))

    # initialize a counter to keep track of how many samples groups were above/below the values for each of the actual perspectives for our keyword group
    keyword_counter = init_k_c()

    keyword_counter = sum_perspectives_for_all_iterations(keyword_counter, all_p, keyword_p, num_iterations)

    print_results(keyword_counter, num_iterations)

    # divide the keyword_counter by the number of iterations to get the percent of randomly sampled groups that have values the differ from the average more significantly than the keywords subgroup
    # if for each, if it is >0.05 then it is not statistically significantly
    keyword_counter = divide_k_c(keyword_counter, num_iterations)

    print_results(keyword_counter, num_iterations)

    save_results(keyword_counter, num_iterations)

    return


if __name__ == "__main__":

    start = time.time()
    print("Program has started\n")

    num_iterations = 25
    run(num_iterations)

    print("Program has ended in: " + str(time.time() - start) + " seconds")