import collections
import json
import logging
import os
import sys


class LoggerWriter:
    '''
    Simple class to redirect output / error streams to a logger.
    '''

    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self._msg = ''

    def write(self, message):
        '''
        Write method to generate clean output
        '''
        self._msg = self._msg + message
        while '\n' in self._msg:
            pos = self._msg.find('\n')
            self.logger.log(self.level, f'(PYTHON: {__name__}.py)  ' + self._msg[:-1])
            self._msg = self._msg[pos + 1:]

    def flush(self):
        '''
        Dummy flush method
        '''
        pass


def setup_logging(snakemake):
    '''
    Setup log formatting with a logger for a Python script called by Snakemake.
    A basic logger is defined first to check if the snakemake object exists,
    then the full logger is initialized.
    '''

    # Setup logging
    logging.basicConfig(level=logging.INFO,
                        format='[%(asctime)s]::%(levelname)s  %(message)s',
                        datefmt='%Y.%m.%d - %H:%M:%S')

    # Check if script was called by snakemake, exit with exception otherwise
    try:
        snakemake
    except NameError:
        logging.error('This script is meant to be called by snakemake.')
        raise

    # Reset logging handler
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Setup logging again with output file
    logging.basicConfig(filename=snakemake.log[0],
                        level=logging.INFO,
                        format='[%(asctime)s]::%(levelname)s  %(message)s',
                        datefmt='%Y.%m.%d - %H:%M:%S')

    # Redirect stdout and stderr to logger object to have all output in log file
    logger = logging.getLogger('logger')
    sys.stdout = LoggerWriter(logger, logging.INFO)
    sys.stderr = LoggerWriter(logger, logging.ERROR)


def create_dirs(output_file_path):
    '''
    Create all intermediate directories in a Snakemake output file.
    '''
    os.makedirs(os.path.dirname(output_file_path))


def remove_file_if_exists(file_path):
    '''
    Removes a file only if this file exists on the file system.
    Return True if the file was erased.
    '''
    if os.path.exists(file_path):
        os.remove(file_path)
        return True
    return False


def touch(file_path, update_timestamp=True):
    '''
    Emulate the 'touch' unix command: create an empty file
    if the file does not exist, update the timestamp otherwise.
    Timestamp update can be disable with update_timestamp=False.
    '''
    if os.path.exists(file_path):
        if update_timestamp:
            os.utime(file_path, None)
        return False
    open(file_path, 'w').close()
    return True


def json_object_hook(obj):
    '''
    Hook for json loads to convert ints and floats properly.
    '''
    converted_values = {}
    for k, v in obj.items():
        if isinstance(v, str):
            try:
                converted_values[k] = int(v)
            except ValueError:
                try:
                    converted_values[k] = float(v)
                except ValueError:
                    converted_values[k] = v
        else:
            converted_values[k] = v
    return converted_values


def load_json_to_dict(input_file_path):
    '''
    Load a json file directly to a dictionary
    '''
    with open(input_file_path) as input_file:
        return json.load(input_file, object_hook=json_object_hook)


def save_dict_to_json(output_file_path, data):
    '''
    Save a dictionary to a json file with nice formatting
    '''
    with open(output_file_path, 'w') as output_file:
        json.dump(data, output_file, indent=4, sort_keys=True)


def is_iterable(variable, exclude_strings=True):
    '''
    Check if variable is an iterable, excluding strings by default
    '''
    if not isinstance(variable, collections.iterable):
        return False
    if isinstance(variable, str) and exclude_strings:
        return False
    return True


def flatten(iterable):
    '''
    Flattens an iterable of iterables into a iterable of single values, e.g.:
    [[1, 2], [3], 4] --> [1, 2, 3, 4]
    '''
    for element in iterable:
        if isinstance(element, collections.abc.Iterable) and not isinstance(element, (str, bytes)):
            yield from flatten(element)
        else:
            yield element
