import time
import pickle
import matplotlib.pyplot as plt


keywords = ['haha', 'joke', 'kek', 'lmao', 'lol', 'lulz', 'rofl']


def plot_table(list, description, path, title):

    # first we round the values
    rounded_list = []
    counter = 1
    for tuple in list:
        rounded_list.append([counter, tuple[0], round(tuple[1], 7), round(tuple[2], 7), round(tuple[3], 7)])
        counter += 1
    list = rounded_list


    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.axis('off')
    ax.axis('tight')

    the_table = plt.table(cellText=list,
                          colLabels=description,
                          loc='center')
    the_table.auto_set_font_size(False)
    the_table.set_fontsize(8)
    # the_table.scale(.7, .7)
    plt.title(title, y=1.1)
    # plt.plot(y)
    # plt.show()
    plt.savefig(f'./Data/Plots/{path}', dpi=1500)


def run():

    coms_in_entire_data_set = 134529233

    time_before_pickle = time.time()
    desc1, all_word_frequencies_cntr = pickle.load(open("./Data/Cooccurence/word_frequency_by_com.pkl", "rb"))
    print(f"Unpickled data in {time.time() - time_before_pickle} seconds. Loaded: {desc1}")

    time_before_pickle = time.time()
    desc2, cntr_for_keywords = pickle.load(open("./Data/Cooccurence/keyword_word_frequency_by_com.pkl", "rb"))
    print(f"Unpickled data in {time.time() - time_before_pickle} seconds. Loaded: {desc2}")


    keyword_cooccurences = {}
    for key in keywords:
        keyword_cooccurences[key] = []

    # initialize values for the difference in likelihood of any word given the presence of a keyword
    for key in keywords:

        coms_with_keyword = cntr_for_keywords.get(key)[0]

        for word in cntr_for_keywords.get(key)[1]:
            likelihood_given_keyword = cntr_for_keywords.get(key)[1].get(word)/coms_with_keyword
            likelihood_in_entire_data_set = all_word_frequencies_cntr.get(word)/coms_in_entire_data_set
            keyword_cooccurences.get(key).append( (word, likelihood_given_keyword-likelihood_in_entire_data_set, likelihood_given_keyword, likelihood_in_entire_data_set) )


    # make tables for all least and most more likely words for each keyword subset
    for key in keywords:
        keyword_cooccurences.get(key).sort(key=lambda x: x[1])
        list_for_table = []
        num_in_table = 25
        for i in range(num_in_table):
            list_for_table.append(keyword_cooccurences.get(key)[i])
        desc_for_table = ["", "Word", "Change in Prob", f"Prob. Given {key}", "Prob. in General"]
        name = f"{key}_{num_in_table}_LEAST_more_likely"
        title = f"Least More Likely Words to Appear with '{key}'"
        plot_table(list_for_table, desc_for_table, name, title)

        keyword_cooccurences.get(key).sort(key=lambda x: x[1], reverse=True)
        list_for_table = []
        num_in_table = 25
        for i in range(num_in_table):
            list_for_table.append(keyword_cooccurences.get(key)[i])
        desc_for_table = ["", "Word", "Change in Prob", f"Prob. Given {key}", "Prob. in General"]
        name = f"{key}_{num_in_table}_MOST_more_likely"
        title = f"Most More Likely Words to Appear with '{key}'"
        plot_table(list_for_table, desc_for_table, name, title)


    # sort the differences to find the largest which refers to:
    # the words which become dispraportionately more likely given presence of a keyword
    # and print the words that are most more likely given a keyword
    # and save initial results to a .txt file
    file_to_save = open("./Data/Cooccurence/initial_results.txt", "a")
    for key in keywords:
        # save the most more likely
        keyword_cooccurences.get(key).sort(key=lambda x: x[1], reverse=True)
        num_to_print = 50
        print(f"Given {key}, {num_to_print} words most more likely to occur are: ")
        file_to_save.write(f"Given {key}, {num_to_print} words MOST more likely to occur are: \n")
        print("Format: (word, likelihood_given_keyword-likelihood_in_entire_data_set, likelihood_given_keyword, likelihood_in_entire_data_set)")
        file_to_save.write("Format: (word, likelihood_given_keyword-likelihood_in_entire_data_set, likelihood_given_keyword, likelihood_in_entire_data_set)\n")
        for i in range(num_to_print):
            print(keyword_cooccurences.get(key)[i])
            file_to_save.write(f"{keyword_cooccurences.get(key)[i]}\n")
        print()
        file_to_save.write(f"\n")

        # save the least  more likely
        keyword_cooccurences.get(key).sort(key=lambda x: x[1])
        print(f"Given {key}, {num_to_print} words least more likely to occur are: ")
        file_to_save.write(f"Given {key}, {num_to_print} words LEAST more likely to occur are: \n")
        print(
            "Format: (word, likelihood_given_keyword-likelihood_in_entire_data_set, likelihood_given_keyword, likelihood_in_entire_data_set)")
        file_to_save.write(
            "Format: (word, likelihood_given_keyword-likelihood_in_entire_data_set, likelihood_given_keyword, likelihood_in_entire_data_set)\n")
        for i in range(num_to_print):
            print(keyword_cooccurences.get(key)[i])
            file_to_save.write(f"{keyword_cooccurences.get(key)[i]}\n")
        print()
        file_to_save.write(f"\n")

    return


if __name__ == "__main__":


    start = time.time()
    print("Program has started\n")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")