"""
Author: Edivaldo Junior
Date:


"""


import sys



def hello1(args):
    """
    To print each element in args passed by cmd line

    INPUT:
        args: value passed by used in cmd 

    OUTPUT:
        none: only print values on cmd
    """
    for i in args:

        print("op1: " + i)
    disaster_response_pipeline
    pass


if __name__ == "__main__":
    
    # avoid to print the first element, witch correspond o python file name executed
    hello1(sys.argv[1:])
