# 4chan_Analysis
Code used during the completion of Senior Honors Thesis at Amherst College, 2020. Looking at instances of humorous and non-humorous language on 4chan's /pol/ boards. Brief descriptions for each file listed below.

Analysis performed using a public dataset of 4chan's /pol/ board (https://zenodo.org/record/3606810).


save_data_streaming_pickle: 
Reformats the data file


clean_data_streaming_multiprocessed:
Cleans each line of text in the data


compare_perspectives:
Finds average eprspectives for keyword subsets

perspectives_for_random_sample:
Repeats the above process for a random sample


make_engagements_to_plot:
Finds the engagement level for each thread 

plot_engagement:
Plots thread engagement


find_entities:
Finds the named entities in each post

examine_entities:
Plots the results 


make_all_unique_words:
Makes a Counter for all unique words in the data

make_keyword_unique_words:
Makes Counters for each subset of data containing a keyword

make_bigram_counter:
Makes a counter of bigrams for each keyword  

examine_keyword_cooccurence:
Plots table of cooccurence results

examine_bigrams:
Plots table of bigram results
