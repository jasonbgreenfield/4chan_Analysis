import time
import datetime
import ast
import pickle


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


def run():

    num_none_times = 0

    # list of tuples (x, y)
    # x = number of seconds the thread survived (latest_post - op_post)
    # y = number of replies
    to_plot = []

    # initialize keyword_to_plots
    keyword_to_plot = {}
    for key in keywords:
        keyword_to_plot[key] = []

    # initialize to_plot with data for each thread in the data set
    with open("./Data/cleaned_data.txt", "r") as f:
        data_generator = s_load(f)

        num_thrupples = 0

        for thrupple in data_generator:

            if num_thrupples%50000==0:
                print(f"On thrupple {num_thrupples} at {datetime.datetime.now()}")
            thread = thrupple[2].get('posts')
            archive_time = 0
            op_time = 0
            num_replies = 0
            first_post_in_thread = True
            for post in thread:

                if first_post_in_thread:
                    try:
                        op_time = post.get('time')
                    except:
                        print("No op_time!!")
                    try:
                        archive_time = post.get('archived_on')
                    except:
                        print("No archive time!!!")
                    num_replies = post.get('replies')

                    first_post_in_thread = False
                    break
                else:
                    break
            if (archive_time is None) or (op_time is None):
                num_none_times += 1
                continue
            thread_engagement = [(archive_time-op_time)/60, num_replies]
            to_plot.append(thread_engagement)


            # find the coms with keywords in them
            cleaned_coms = thrupple[1]
            # initialize keyword_counts
            keyword_counts = {}
            for key in keywords:
                keyword_counts[key] = [0]

            for uid, com in cleaned_coms:
                for key in keywords:
                    if key in com:
                        keyword_counts.get(key)[0] += 1

            # append data to the appropriate {keyword}_to_plot
            for key in keywords:
                if keyword_counts.get(key)[0] > 0:
                    keyword_to_plot.get(key).append([thread_engagement[0], thread_engagement[1], keyword_counts.get(key)[0]])

            num_thrupples += 1

    print(f"Num NoneTypes in either archive or op time: {num_none_times}")

    # plot to_plot
    print(f"to_plot: {to_plot[:50]}")

    # we will also save to_plot so we can plot quicker next time!
    start_time = time.time()
    print("about to normal pickle dump the to_plot data")
    pickle_out = open("./Data/Engagement/all_engagement_to_plot.pkl", "wb")
    desc = f"This is a list of lists. Each sublist has two values. Minutes a thread survives. Second is number of replies a thread has. (Made on {datetime.datetime.now()})."
    pickle.dump((desc, to_plot), pickle_out)
    pickle_out.close()
    print(f"Succesfully saved all unique words in data set in {time.time() - start_time} seconds\n")

    # now we save all the keyword_to_plots too
    for key in keywords:
        to_save = keyword_to_plot.get(key)
        print(f"{key}_to_save: {to_save[:50]}")
        start_time = time.time()
        print(f"about to normal pickle dump the {key}_to_plot data")
        pickle_out = open(f"./Data/Engagement/{key}_engagement_to_plot.pkl", "wb")
        desc = f"For all threads with {key} in at least one of its comments. This is a list of lists. Each sublist has three values. l[0] = Minutes a thread survives. l[1] =  number of replies a thread has. l[2] = number of commentsd with {key} in it. (Made on {datetime.datetime.now()})."
        pickle.dump((desc, to_save), pickle_out)
        pickle_out.close()
        print(f"Succesfully saved all unique words in data set in {time.time() - start_time} seconds\n")

    return


if __name__ == "__main__":

    start = time.time()
    print("Program has started\n")

    run()

    print(f"Finished program in {time.time() - start} seconds")