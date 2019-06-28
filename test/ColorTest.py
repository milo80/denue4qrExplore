import numpy as np
import random


from functions.FooPlot import *


if __name__ == '__main__':

    # Create class Object

    keys_cat = ['Taco Place',
        'Mexican Restaurant',
        'Restaurant',
        'Pharmacy',
        'Medical Center',
        'Fast Food Restaurant',
        'Church',
        'Gas Station',
        'Internet Cafe',
        'Gym'
    ]

    color = rand_color(4)
    print(color)

    rand_color_fixed(4)
    Color = create_color_palette(keys_cat)



