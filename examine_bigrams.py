import time
import pickle
import matplotlib.pyplot as plt

keywords = ['haha', 'joke', 'kek', 'lmao', 'lol', 'lulz', 'rofl']


def plot_table(list, description, path, title):

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.axis('off')
    ax.axis('tight')

    the_table = plt.table(cellText=list,
                          colLabels=description,
                          loc='center')
    the_table.auto_set_font_size(False)
    the_table.set_fontsize(8)
    the_table.scale(.6, 1.6)
    plt.title(title, y=1.1)
    # plt.show()
    plt.savefig(f'./Data/Plots/Bi-Grams/{path}', dpi=1500)


def examine_bigrams():

    num_to_plot = 15
    test = ['kek']
    for key in test:
        desc, counted_bigrams = pickle.load(open(f"./Data/BiGrams/{key}_bigrams.pkl", "rb"))

        top_bigrams = counted_bigrams.most_common(num_to_plot)
        to_plot = []
        for i in range(len(top_bigrams)):
            to_plot.append([(i+1), top_bigrams[i][0], top_bigrams[i][1]])

        desc_for_table = ["", "Bigram", "Frequency"]
        name = f"{key}_top_{num_to_plot}_bigrams"
        title = f"Most Common Bigrams for '{key}'"
        plot_table(to_plot, desc_for_table, name, title)

    return


def run():

    examine_bigrams()

    return


if __name__ == "__main__":
    start = time.time()
    print("Program has started")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")