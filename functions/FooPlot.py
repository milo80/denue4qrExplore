import matplotlib.pyplot as plt
import random


# Fixed numbers
def rand_color_fixed(number_of_colors):
    choise_range = '04589ACF'
    choise_range1 = '05E2F19'
    choise_range2 = '17AF3D0'
    choise_range3 = '40F0A3C'

    color = ["#"+''.join([random.choice(choise_range1) +
                          ''.join([random.choice(choise_range)]) +
                          ''.join([random.choice(choise_range2)]) +
                          ''.join([random.choice(choise_range)]) +
                          ''.join([random.choice(choise_range3)]) +
                          ''.join([random.choice(choise_range)])
                          for j in range(1)])
             for i in range(number_of_colors)]

    #print(color)
    return color


def rand_color(number_of_colors):
    choise_range = '0123456789ABCDEF'
    color = ["#"+''.join([random.choice(choise_range) for j in range(6)])
             for i in range(number_of_colors)]

    return color


# returns dictionary of colors by category
def create_dict_colors(keys) -> dict:
    cat_color = {}
    k = len(keys)
    colors = rand_color_fixed(k)
    for i in range(0, k):
        cat_color[keys[i]] = colors[i]

    cat_color.update([('Other', '#A4A4A4')])
    return cat_color


# returns a dictionary of catetorie key and random colors
# testing purpose
def create_color_palette(keys) -> dict:
    cat_color = {}
    k =len(keys)
    colors = rand_color_fixed(k)
    for i in range(k):
        plt.scatter(random.randint(0, 10), random.randint(0,10), c=colors[i], s=200)

    plt.show()
    for i in range(0, k):
        cat_color[keys[i]] = colors[i]

    print(cat_color)
    return cat_color


# Pie plot iterable
def pie_plot_iterated(df: object):
    return df.printSchema()
