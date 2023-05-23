import logging
import configparser

def latin_square(n):
    return [[(i+j)%n for i in range(n)] for j in range(n)]

def configuration(filename='./configuration.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config
