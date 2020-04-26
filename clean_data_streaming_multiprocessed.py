import ast
import time
import nltk
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.corpus import words
import string
import datetime
from multiprocessing import Pool



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


def remove_bad_parts_of_words(list):

    second_words = []

    for word in list:
        if '</span><br>' in word:
            # some words in data are tokenized as: 'belongings</span><br>yeah'
            # when they should really be: 'belongings' 'yeah'
            # this fixes that
            new_two = word.split('</span><br>')
            second_words.append(new_two[0])
            second_words.append(new_two[1])

        elif '&#039;' in word:
            # contraction_parts = word.split('&#039;')
            # new_word = contraction_parts[0] + "'" + contraction_parts[1]
            new_word = word.replace("&#039;", "'")
            second_words.append(new_word)
            # print(f"old: {word} and new: {new_word}")

        elif '&quot;' in word:
            new_word = word.replace("&quot;", "'")
            second_words.append(new_word)
            # print(f"old: {word} and new: {new_word}")

        elif '</a><br' in word:
            new_two = word.split('</a><br')
            second_words.append(new_two[1])
            # print(f"old: {word} and new: {new_two[1]}")

        elif 'class="quote">&gt;' in word:
            new_two = word.split('class="quote">&gt;')
            second_words.append(new_two[1])

        elif '<br><br><' in word:
            new_two = word.split('<br><br><')
            second_words.append(new_two[0])
            second_words.append(new_two[1])

        elif '<br><br>' in word:
            new_two = word.split('<br><br>')
            second_words.append(new_two[0])
            second_words.append(new_two[1])

        elif '<br><' in word:
            new_two = word.split('<br><')
            second_words.append(new_two[0])
            second_words.append(new_two[1])

        elif '<br>' in word:
            new_two = word.split('<br>')
            second_words.append(new_two[0])
            second_words.append(new_two[1])

        elif '</span>' in word:
            new_two = word.split('</span>')
            second_words.append(new_two[0])

        elif ('br' in word) and (word not in words.words()):
            new_two = word.split('br')
            second_words.append(new_two[1])

        elif '<' in word:
            new_two = word.split('<')
            second_words.append(new_two[1])

        elif 'href' in word:
            continue

        else:
            second_words.append(word)

    return second_words


def clean_line(line):

    tokenized_words = line.split()
    clean_words = []

    stop_words = set(stopwords.words('english'))
    bad_words = ['href', '<a', '<br', '<span', 'span>', 'quotelink', 'class=', '&quot', '&#', 'classquot' 'classquotegtim']

    # convert bad characters to legible words first
    # do this twice to catch everything
    cleaner_words = remove_bad_parts_of_words(tokenized_words)
    cleaner_words = remove_bad_parts_of_words(cleaner_words)


    # remove punctuation from each word
    table = str.maketrans('', '', string.punctuation)
    cleaner_words = [w.translate(table) for w in cleaner_words]

    # stem words
    porter = PorterStemmer()
    cleaner_words = [porter.stem(word) for word in cleaner_words]

    for word in cleaner_words:
        if word not in stop_words:
            num_good = 0
            for bad in bad_words:
                if bad not in word:
                    num_good += 1
            if num_good is len(bad_words):
                clean_words.append(word.lower())

    return clean_words


def clean_thread(line):

    # this is our cleaned data
    cleaned_coms = []
    # thread_uid is a list of every  uid of all coms in that thread
    thread_uid = []
    num_errors = 0

    try:
        thread = line.get('posts')

        for post in thread:
            try:
                com = post.get('com')
                com_uid = post.get('no')
                cleaned_coms.append( (com_uid, clean_line(com)) )
                thread_uid.append(com_uid)
            except Exception as e:
                num_errors += 1

    except Exception as e:
        num_errors += 1

    # if num_errors > 0:
    #     # NOTE: even if there is an error with a single comment in a thread, all the other comments from the thread are still cleaned and saved
    #     print(f"In clean_thread, num_errors: {num_errors}. on Thread: {thread}")

    return (thread_uid, cleaned_coms, line)


def run():

    with open("./Data/streaming_pickle_data.txt", "r") as streaming_data:
        counts = 0
        data_generator = s_load(streaming_data)

        # We're using vm=final-uploader with 24gbRAM and 18vcpu's
        # so let's do batches of 72 lines of the data file
        # then multiprocess the cleaning of those with ~18 workers
        # Then save all of those
        # and repeat
        batch_to_clean = []
        for line in data_generator:

            batch_to_clean.append(line)

            if len(batch_to_clean)==72:
                # clean data
                p = Pool(processes=None)
                cleaned_batch = p.map(clean_thread, (thread for thread in batch_to_clean))
                p.close()

                file_saving_to = open("./Data/cleaned_data.txt", "a")
                s_dump(cleaned_batch, file_saving_to)
                file_saving_to.close()
                batch_to_clean.clear()

            if counts%50000==0:
                print(f"Cleaned up to thread {counts} by {datetime.datetime.now()}")
                # if len(cleaned_line_batch_of_five)>0:
                #     print(f"Cleaned thread {counts}: {cleaned_line_batch_of_five[0]}")
                #     print()


            counts += 1


        # after loop through data set, check to see how many lines are in the last batch, and save those if there are any
        if len(batch_to_clean)>0:

            file_saving_to = open("./Data/cleaned_data.txt", "a")
            s_dump(cleaned_batch, file_saving_to)
            file_saving_to.close()
            batch_to_clean.clear()

    return

if __name__ == "__main__":

    start = time.time()
    nltk.download('stopwords')
    print("Program has started\n")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")