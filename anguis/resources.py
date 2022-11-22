'''
Utilities for resources allocation.
'''

import collections
import re
import yaml


class SnakemakeResourcesManager():
    '''
    '''

    default_resources = {'threads': 1,
                         'mem_mb': 2000,
                         'runtime_s': 3600}

    def __init__(self, resources=None, presets=None, override=None,
                 mem_attempt_multiplier=1.5, runtime_attempt_multiplier=1,
                 default_str='default', threads_str='threads', memory_str='mem_mb', runtime_str='runtime_s'):
        '''
        '''
        self._default_str = default_str  # Name of default resources values in resources definition dict
        self._threads_str = threads_str  # String for number of threads in resources definition dict
        self._memory_str = memory_str  # String for memory in resources definition dict
        self._runtime_str = runtime_str  # String for runtime in resources definition dict
        self.mem_attempt_multiplier = mem_attempt_multiplier  # Memory for a job will be multiplied by this value for each new attempt
        self.runtime_attempt_multiplier = runtime_attempt_multiplier  # Runtime for a job will be multiplied by this value for each new attempt
        self.defined_resources = resources  # Dictionary containing resources as defined by the workflow / user
        self.presets = None  # Resources presets dictionary if presets were defined
        # Populate resources with default values defined in the class definition
        # if resources are not provided.
        if self.defined_resources is None:
            self.defined_resources = {}
        if self._default_str not in self.defined_resources:
            self.defined_resources[self._default_str] = {threads_str: self.default_resources['threads'],
                                                   memory_str: self.default_resources['mem_mb'],
                                                   runtime_str: self.default_resources['runtime_s']}
        self.resources = collections.defaultdict(lambda: collections.defaultdict(int))  # Dictionary with final parsed resources

    def get_resource_value(self, rule, resource):
        '''
        Return the value of a resource setting if the value was specified directly
        or the preset value if a preset was used.
        '''
        try:
            value = self.defined_resources[rule][resource]
            # Get value from presets if defined that way
            try:
                value = int(value)
            except ValueError:
                try:
                    value = self.presets[resource][value]
                except ValueError as e:
                    print(f'Value {value} for resource {resource} is neither a number nor a preset.')
                    raise
        except KeyError:  # Value was not defined for this resource, use default
            value = self.defined_resources[self._default_str][resource]
            print(f'Could not find {resource} specification for rule {rule}. Using default ({value})')
        self.resources[rule][resource] = value

    def allocate_resources(self):
        '''
        Get resources requirements for all rules from the resources defined in config.yaml.
        '''
        # Get default resources from resources dict
        self.resources[self._default_str] = {self._threads_str: get_resource_value(self._default_str, self._threads_str),
                                             self._memory_str: get_resource_value(self._default_str, self._memory_str),
                                             self._runtime_str: get_resource_value(self._default_str, self._runtime_str)}
        for rule in self.defined_resources:
            if rule == self._default_str:  # Default values were already loaded before
                continue
            # Get each resource value for the rule
            self.get_resource_value(rule, self._threads_str)
            self.get_resource_value(rule, self._memory_str)
            self.get_resource_value(rule, self._runtime_str)
        # Override resource values from the definition file with values from the config file
        if resources_info['override']:
            for rule, resources in resources_info['override'].items():
                for resource, value in resources.items():
                    try:
                        self.resources[rule][resource] = get_resource_value(resource, value, presets)
                    except KeyError:
                        print(f'Invalid resource <{resource}> or rule name <{rule}> in resources section of the config file')

    def get_threads(self, rule):
        '''
        Get number of threads for a rule from the config dictionary.
        '''
        try:
            threads = self.resources[rule][self._threads_str]
        except KeyError:
            threads = self.resources[self._default_str][self._threads_str]
        return threads

    def get_mem(self, rule, attempt):
        '''
        Get memory requirement for a rule from the config dictionary.
        Memory is increased 1.5x per attempt.
        '''
        try:
            mem_mb = self.resources[rule][self._memory_str]
        except KeyError:
            mem_mb = self.resources[self._default_str][self._memory_str]
        if isinstance(mem_mb, (int, float)):
            mem_mb = int(mem_mb * (self.mem_attempt_multiplier ** (attempt - 1)))
        elif mem_mb.isdigit():
            mem_mb = int(int(mem_mb) * (self.mem_attempt_multiplier ** (attempt - 1)))
        elif mem_mb[-1] in ('G', 'M', 'K'):  # Careful, this cannot be used in resources (not an int)
            tmp = float(mem_mb[:-1]) * (self.mem_attempt_multiplier ** (attempt - 1))
            mem_mb = f'{tmp}{mem_mb[-1]}'
        return mem_mb

    def get_runtime(self, rule, attempt):
        '''
        Get runtime requirement for a rule from the config dictionary.
        Runtime is increased 1.5x per attempt.
        '''
        try:
            runtime = self.resources[rule][self._runtime_str]
        except KeyError:
            runtime = self.resources[self._default_str][self._runtime_str]
        if isinstance(runtime, int):
            time = runtime
        else:
            try:
                d, h, m, s = (int(f) for f in re.split(':|-', runtime))
                time = ((((d * 24) + h) * 60) + m) * 60 + s
            except ValueError:
                print(f'Invalid runtime format for rule <{rule}>: <{runtime}>')
                time = 3600
        time = int(time * (1 + (attempt - 1) / 2))  # Remove this at the moment since curnagl has such a low max time limit
        return time
