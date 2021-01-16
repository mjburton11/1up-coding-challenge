import pandas as pd
import os
import functools
from tabulate import tabulate
import sys

class RescourceDataObj(object):
    def __init__(self, filename, directory='data'):
        self.filename = filename
        self.directory = directory

        self.counter_ids = list()
        self._extension = '.ndjson'

        self.resource_name = self.extract_resource_name()
        self.df = self.load_data()

        self._reference_map = None
        self._resource_dict_map = dict()

    @property
    def number_of_calls(self):
        return len(self.counter_ids)

    @property
    def reference_map(self):
        if self._reference_map is None:
            self._reference_map = self.make_reference_map()
        return self._reference_map

    def make_reference_map(self):
        ref_map = {}
        for col in self.df:
            col_df = self.df[col].map(self.search_is_reference).explode()
            if any(col_df.values):
                unique_vals = col_df.unique()
                ref_map[col] = unique_vals[unique_vals != False]
        return ref_map

    def resource_dict(self, resource):
        if resource not in self._resource_dict_map.keys():
            self._resource_dict_map[resource] = self.build_resource_dict(resource)
        return self._resource_dict_map[resource]

    def build_resource_dict(self, resource):
        # figure out column name that goes in the dataframe
        #
        colname = [c for c, rsrcs in self.reference_map.items() if resource in rsrcs]
        if len(colname) == 0:
            return None

        # assuming here that if a resource type appears twice in the same entry that it is
        # has the same id.  This seems to be correct...
        colname = colname[0]

        dfr = pd.concat([self.df['id'], self.df[colname].map(functools.partial(
            self.explode_search, criteria_func=functools.partial(
                self.search_resource_criteria, resource_name=resource))).explode()], axis=1).rename(
            columns={'id': 'id', colname: resource})

        return dfr

    def load_data(self):
        f = open(self.directory + os.sep + self.filename, 'r')
        df = pd.read_json(f, lines=True)
        f.close()
        return df

    def extract_resource_name(self):
        return self.filename.replace(self._extension, '')

    def explode_search(self, cell, criteria_func=None, cell_list=None):
        if cell_list is None:
            cell_list = []
        if isinstance(cell, list):
            for c in cell:
                self.explode_search(c, criteria_func=criteria_func, cell_list=cell_list)
        elif isinstance(cell, dict):
            for key, c in cell.items():
                if isinstance(c, dict) or isinstance(c, list):
                    self.explode_search(c, criteria_func=criteria_func, cell_list=cell_list)
                else:
                    criteria_func(c, cell_list)
        else:
            criteria_func(cell, cell_list)
        return cell_list

    def seach_list_criteria(self, cell, cell_list, list_criteria=None):
        if cell in list_criteria:
            cell_list.append(cell)

    def search_resource_criteria(self, cell, cell_list, resource_name=None):
        if isinstance(cell, str) and resource_name + '/' in cell:
            cell_list.append(cell.replace(resource_name + '/', ''))

    def search_is_reference(self, cell, refs=None):
        if refs is None:
            refs = [False]
        if isinstance(cell, list):
            for c in cell:
                self.search_is_reference(c, refs=refs)
        elif isinstance(cell, dict):
            for key, item in cell.items():
                if key == 'reference' and '/' in item:
                    refs.append(item.split('/')[0])
                elif isinstance(item, list) or isinstance(item, dict):
                    self.search_is_reference(item, refs=refs)
        return refs

class PatientResourceDataObj(RescourceDataObj):
    def __init__(self, *args, **kwargs):
        super(PatientResourceDataObj, self).__init__(*args, **kwargs)

        self._name_col = 'name'

    def set_patient_id(self, firstname, lastname, id):
        if id is None:
            id = self.find_patient_id(firstname, lastname)

        self.input_id = id

    def find_patient_id(self, firstname, lastname):
        id = None
        for id, cell in zip(self.df.id, self.df[self._name_col]):
            name = ''.join(self.explode_search(cell, criteria_func=functools.partial(
                self.seach_list_criteria, list_criteria=[firstname, lastname])))
            if name != '':
                break
        return id

def load_all_data(data_directory='data'):
    full_data = dict()

    files = os.listdir(data_directory)
    for filename in files:
        if 'Patient' in filename:
            rdo = PatientResourceDataObj(filename)
        else:
            rdo = RescourceDataObj(filename)

        full_data[rdo.resource_name] = rdo

    return full_data

def resource_counter_cli(firstname, lastname, id, verbose=True):
    data = load_all_data()

    patient_key = 'Patient'

    if id is None:
        data[patient_key].set_patient_id(firstname, lastname, None)
        id = data[patient_key].input_id

    resource_types = data.keys()

    for resource in resource_types:
        rdo = data[resource]

        if resource == patient_key:
            rdo.counter_ids.append([rdo.input_id])

        has_patients = False
        df_pats = rdo.resource_dict(patient_key)
        if df_pats is not None:
            df_pat = df_pats[(df_pats[patient_key] == id) & (~df_pats.id.isin(rdo.counter_ids))]
            rdo.counter_ids.extend(list(df_pat.id.unique()))
            has_patients = True

        for ref_key, ref_resources in rdo.reference_map.items():
            for ref_rs in ref_resources:
                if patient_key == ref_rs:
                    continue
                if has_patients:
                    df_ref = rdo.resource_dict(ref_rs)
                    ref_ids = df_ref[(df_ref.id.isin(df_pat.id)) & (
                        ~df_ref[ref_rs].isin(data[ref_rs].counter_ids)) & (
                        ~df_ref[ref_rs].isnull())][ref_rs].unique()
                    data[ref_rs].counter_ids.extend(list(ref_ids))

    counter_table = [(resource, data[resource].number_of_calls) for resource in
                     sorted(data.keys(), key=lambda x: data[x].number_of_calls, reverse=True)]

    if verbose:
        print(tabulate(counter_table, headers=['Resource Type', 'Count']))

    return counter_table

    return id

def command_line_arg_run(sys_args):

    arg_dict = {'firstname': None,
                'lastname': None,
                'id': None}

    for i, sa in enumerate(sys_args):
        inp = sa.replace('-', '')
        if inp in arg_dict.keys():
            arg_dict[inp] = sys_args[i+1]

    if all([v is not None for v in arg_dict.values()]):
        raise ValueError('Must specify either names or id')
    elif all([v is None for v in arg_dict.values()]):
        raise ValueError('Must specify either names or id')
    elif arg_dict['firstname'] is not None and arg_dict['lastname'] is None:
        raise ValueError('Must specify first and last name')
    elif arg_dict['firstname'] is None and arg_dict['lastname'] is not None:
        raise ValueError('Must specify first and last name')
    elif arg_dict['firstname'] is not None and arg_dict['id'] is not None:
        raise ValueError('Must specify either names or id')
    elif arg_dict['lastname'] is not None and arg_dict['id'] is not None:
        raise ValueError('Must specify either names or id')

    if arg_dict['firstname'] is not None:
        resource_counter_cli(firstname=arg_dict['firstname'],
                             lastname=arg_dict['lastname'], id=None)
    else:
        resource_counter_cli(firstname=None, lastname=none, id=arg_dict['id'])

if __name__ == "__main__":
    command_line_arg_run(sys.argv)
