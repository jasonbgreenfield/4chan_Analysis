import time
import numpy as np
import pandas as pd
import datashader as ds
import holoviews as hv
import holoviews.operation.datashader as hd
import pickle

import matplotlib.pylab as plt

keywords = ['haha', 'joke', 'kel', 'lmao', 'lol', 'lulz', 'rofl']


def plot_keyword_engagement(key, data):

    # make a pd.DataFrame with list of lists and the names for each column
    df = pd.DataFrame(data, columns=['Thread Lifespan (min)', 'Num Replies', 'Keyword Counts'])
    print(f"Plotting engagement for: {key}")

    hd.shade.cmap = ["lightblue", "darkblue"]
    hv.extension("bokeh", "matplotlib")

    hv.output(backend="matplotlib")
    agg = ds.Canvas().points(df, 'Thread Lifespan (min)', 'Num Replies')
    hvimg = hd.shade(hv.Image(agg))
    hv.save(hvimg, f'./Data/Plots/{key}_engagement_plot.png', dpi=1500)

    return


def plot(data):

    # make a pd.DataFrame with list of lists and the names for each column
    df = pd.DataFrame(data, columns=['Thread Lifespan (min)', 'Num Replies'])

    hd.shade.cmap = ["lightblue", "darkblue"]
    hv.extension("bokeh", "matplotlib")

    hv.output(backend="matplotlib")
    agg = ds.Canvas().points(df, 'Thread Lifespan (min)', 'Num Replies')
    hvimg = hd.shade(hv.Image(agg))
    hv.save(hvimg, './Data/Plots/engagement_datashader.png', dpi=1500)


def keyword_matplot_scatter(key, data, min_thresh):

    x_list = []
    y_list = []
    alphas_list = []
    max_alpha = 0
    for thrupple in data:
        x_list.append(thrupple[0])
        y_list.append(thrupple[1])
        alphas_list.append(thrupple[2])
        if thrupple[2] > max_alpha:
            max_alpha = thrupple[2]

    # normalize alphas
    for i in range(len(alphas_list)):
        alphas_list[i] = alphas_list[i]/max_alpha


    x = np.array(x_list)
    y = np.array(y_list)
    alphas = np.array(alphas_list)

    rgba_colors = np.zeros((len(data), 4))
    # for red the first column needs to be one
    rgba_colors[:, 2] = 1.0
    # the fourth column needs to be your alphas
    rgba_colors[:, 3] = alphas

    plt.scatter(x, y, color=rgba_colors)

    plt.title(f"{key} Engagement, Minimum Keywords Per Thread: {min_thresh}")
    plt.xlabel("Thread Lifespan (min)")
    plt.ylabel("Num Replies")
    # plt.show()
    plt.savefig(f'./Data/Plots/{key}_scatter_min{min_thresh}', dpi=1500)


def keyword_hexbin(key, data):
    x_list = []
    y_list = []
    for thrupple in data:
        x_list.append(thrupple[0])
        y_list.append(thrupple[1])

    x = np.array(x_list)
    y = np.array(y_list)

    hb = plt.hexbin(x, y, cmap='viridis')
    cb = plt.colorbar(hb)
    cb.set_label('counts')

    # plt.show()
    plt.title(f"{key} Engagement")
    plt.xlabel("Thread Lifespan (min)")
    plt.ylabel("Num Replies")
    plt.savefig(f'./Data/Plots/{key}_hex', dpi=1500)

    return


def plot_scatter_all(data):
    x_list = []
    y_list = []
    for tuple in data:
        x_list.append(tuple[0])
        y_list.append(tuple[1])

    x = np.array(x_list)
    y = np.array(y_list)

    print(f"X list: {x}")
    print(f"Y list: {y}")

    plt.scatter(x, y, color='blue', s=3)
    # plt.hexbin(x, y)
    plt.xlim(0, 2000)
    plt.ylim(900, 2200)
    plt.title(f"All Thread Engagement, Third Cluster")
    plt.xlabel("Thread Lifespan (min)")
    plt.ylabel("Num Replies")
    # plt.show()
    plt.savefig(f'./Data/Plots/all_threads_scatter', dpi=1500)


def plot_all():

    # plot all engagement
    time_before_pickle = time.time()
    desc, all_engagement_to_plot = pickle.load(open("./Data/Engagement/all_engagement_to_plot.pkl", "rb"))
    print(f"Unpickled data in {time.time() - time_before_pickle} seconds. Loaded: {desc}")


    # # remove threads with lifespan > 3000
    # # remove threads with num_replies > 1500
    xmin = 0
    xmax = 1000
    ymin = 0
    ymax = 600
    subset_to_plot = []
    for tuple in all_engagement_to_plot:
        if ((tuple[0] > xmin) and (tuple[0] < xmax)) and ( (tuple[1] > ymin) and (tuple[1] < ymax)):
            subset_to_plot.append(tuple)

    plot_scatter_all(subset_to_plot)

    keyword_hexbin('All Threads', subset_to_plot)

    return


def plot_keywords():
    # plot engagement
    # upper bounds for each key to remove from
    keyword_bounds = {'haha': [500, 500], 'joke': [550, 450], 'kek': [550, 450], 'lmao': [450, 550], 'lol': [450, 500],
                      'lulz': [350, 150], 'rofl': [100, 500], }


    for key in keywords:
        time_before_pickle = time.time()
        desc, keyword_to_plot = pickle.load(open(f"./Data/Engagement/{key}_engagement_to_plot.pkl", "rb"))
        print(f"Unpickled data in {time.time() - time_before_pickle} seconds. Loaded: {desc}")


        # plot datashader for all keyword threads
        only_two_columns = []
        for thrupple in keyword_to_plot:
            if (thrupple[0] < keyword_bounds.get(key)[0]) and (thrupple[1] < keyword_bounds.get(key)[1]):
                only_two_columns.append( (thrupple[0], thrupple[1]) )

        plot(only_two_columns)

        plot_keyword_engagement(key, only_two_columns)

        keyword_hexbin(key, only_two_columns)

    return


def run():

    plot_all()

    plot_keywords()

    return



if __name__ == "__main__":

    start = time.time()
    print("Program has started\n")

    run()

    print(f"Finished program in {time.time() - start} seconds")