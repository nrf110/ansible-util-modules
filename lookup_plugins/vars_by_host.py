from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from ansible.plugins.lookup import LookupBase
from ansible.errors import AnsibleError
from ansible.inventory import Inventory
import json

def merge(a, b, path=None, update=True):
    "http://stackoverflow.com/questions/7204805/python-dictionaries-of-dictionaries-merge"
    "merges b into a"
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            elif isinstance(a[key], list) and isinstance(b[key], list):
                for idx, val in enumerate(b[key]):
                    a[key][idx] = merge(a[key][idx], b[key][idx], path + [str(key), str(idx)], update=update)
            elif update:
                a[key] = b[key]
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

class LookupModule(LookupBase):
    def update_path(self, term, value, target, prev_key=None):
        if term is None: path = []
        if type(term) not in (str, int, list): raise AnsibleError()
        if type(term) in (str, int): path = [term]
        if type(term) is list: path = term

        if path:
            current_key = path[0]

            if prev_key is None:
                self.update_path(path[1:], value, target, current_key)
            elif type(current_key) in (str, int):
                if type(target) is dict:
                    if type(current_key) is str:
                        target.setdefault(prev_key, {})
                    elif type(current_key) is int:
                        target.setdefault(prev_key, [])
                elif type(target) is list:
                    if type(current_key) is str:
                        target.insert(prev_key, {})
                    elif type(current_key) is int:
                        target.insert(prev_key, [])

                self.update_path(path[1:], value, target[prev_key], current_key)
            else:
                raise AnsibleError('key: ' + str(current_key) + ' is not a str or int')
        elif prev_key is not None:
            if type(target) is list:
                target.insert(prev_key, value)
            else:
                target[prev_key] = value

    def object_from_path(self, path, value):
        result = {}
        self.update_path(path, value, result)
        return result

    def get_hosts(self, pattern, variables):
        hosts = []

        if pattern[0] in ('!','&'):
            obj = pattern[:1]
        else:
            obj = pattern

        if obj in variables['groups']:
            hosts = variables['groups'][obj]
        elif obj in variables['groups']['all']:
            hosts = [obj]

        return hosts

    def get_var(self, term, src):
        if term is None: path = []
        if type(term) not in (str, int, list): raise AnsibleError('term: ' + str(term) + ' is not a str or int')
        if type(term) in (str, int): path = [term]
        if type(term) is list: path = term

        if path:
            current_key = path[0]
            if (type(src) is dict) and (current_key in src):
                return self.get_var(path[1:], src[current_key])
            elif (type(src) is list) and (type(current_key) is int) and (len(src) > current_key):
                return self.get_var(path[1:], src[current_key])
            else:
                return None
        else:
            return src

    def get_vars(self, host, terms, variables):
        results = [{ 'host': host }]

        for term in terms:
            value = self.get_var(term, variables['hostvars'].get(host, {}))

            if value is not None:
                results.append(self.object_from_path(term, value))

        return reduce(lambda x,y: merge(x,y), results)

    def run(self, terms, variables=None, **kwargs):
        host_list = []

        patterns = Inventory.order_patterns(Inventory.split_host_pattern(terms[0]))

        for pattern in patterns:
            hosts = self.get_hosts(pattern, variables)

            if pattern.startswith("!"):
                host_list = [host for host in host_list if host not in hosts]
            elif pattern.startswith("&"):
                host_list = [host for host in host_list if host in hosts]
            else:
                host_list.extend(hosts)

        return [self.get_vars(host, terms[1:], variables)
                for host in list(set(host_list))]
