import time
import pickle
import matplotlib.pyplot as plt

keywords = ['haha', 'joke', 'kek', 'lmao', 'lol', 'lulz', 'rofl']


def get_top(list):
    to_plot = []
    for i in range(len(list)):
        if i < 25:
            to_plot.append([i + 1, str(list[i][0].rstrip()), list[i][1]])
    return to_plot


def get_bottom(list):
    to_plot = []
    for i in range(len(list)):
        if i > 24:
            to_plot.append([i + 1, str(list[i][0].rstrip()), list[i][1]])
    return to_plot


def reset_fig():
    fig, ax = plt.subplots()

    # hide axes
    fig.patch.set_visible(False)
    ax.axis('off')
    ax.axis('tight')
    return


def plot_table(list, description, key, title):

    top = get_top(list)
    bottom = get_bottom(list)

    reset_fig()

    the_table = plt.table(cellText=top, colLabels=description, loc='center', colWidths= [.12, .25, .25])
    the_table.set_fontsize(8)
    the_table.scale(.6, .9)
    plt.suptitle(title, y=.93,  fontsize=8)
    # plt.show()
    plt.savefig(f'./Data/Entities/{key}_entities_top', dpi=1500)


    reset_fig()

    the_table = plt.table(cellText=bottom, loc='center', colWidths=[.12, .25, .25])
    the_table.set_fontsize(8)
    the_table.scale(.6, .9)
    # plt.show()
    plt.savefig(f'./Data/Entities/{key}_entities_bottom', dpi=1500)



def run():

    desc1, all_entities = pickle.load(open("./Data/Entities/all_entities.pkl", "rb"))
    desc2, haha_entities = pickle.load(open("./Data/Entities/haha_entities.pkl", "rb"))
    desc3, joke_entities = pickle.load(open("./Data/Entities/joke_entities.pkl", "rb"))
    desc4, lmao_entities = pickle.load(open("./Data/Entities/lmao_entities.pkl", "rb"))
    desc5, lol_entities = pickle.load(open("./Data/Entities/lol_entities.pkl", "rb"))
    desc6, lulz_entities = pickle.load(open("./Data/Entities/lulz_entities.pkl", "rb"))
    desc7, rofl_entities = pickle.load(open("./Data/Entities/rofl_entities.pkl", "rb"))
    desc8, kek_entities = pickle.load(open("./Data/Entities/kek_entities.pkl", "rb"))

    data_dict = { 'haha': haha_entities,
                  'joke': joke_entities,
                  'kek': kek_entities,
                  'lmao': lmao_entities,
                  'lol': lol_entities,
                  'lulz': lulz_entities,
                  'rofl': rofl_entities
                  }

    to_print = 50
    columns_desc = ["", "Entity", "Frequency"]

    # plot keyword tables
    for key in keywords:
        top_50 = data_dict.get(key).most_common(to_print)
        title = f"{key}, Top 50 Entities"
        plot_table(top_50, columns_desc, key, title)
    # plot table for all
    title = "Top 50 Entiteis For All Threads"
    all_top_50 = all_entities.most_common(to_print)
    plot_table(all_top_50, columns_desc, 'all', title)

    return


if __name__ == "__main__":

    start = time.time()
    print("Program has started\n")

    run()

    print("Program has ended in: " + str(time.time() - start) + " seconds")