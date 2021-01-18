import pandas as pd
import os
from tabulate import tabulate
from json import loads
import sys

class InstanceRegister:
    def __call__(self, init):
        def register(instance, *args, **kwargs):
            init(instance, *args, **kwargs)
            try :
                instance.__class__.__instances__
            except:
                instance.__class__.__instances__ = []
            instance.__class__.__instances__.append(instance)
        return register

class Resource(object):
    __instances__ = []
    @InstanceRegister()
    def __init__(self, id=None, resource_type=None):
        self.id = id
        self.resource_type = resource_type

class PatientResource(object):
    __instances__ = []
    @InstanceRegister()
    def __init__(self, firstname=None, lastname=None):
        self.firstname = firstname
        self.lastname = lastname

class RescourceDataObj(object):
    """
    Object to organize data structure manipulation and storage
    """
    def __init__(self, filename, directory='data'):
        self.filename = filename
        self.directory = directory

        # counter id will store how many unique times this object
        # is associated with a patient
        self.counter_ids = list()
        self._extension = '.ndjson'
        self._json_search_keyname = 'reference'
        self._resource_type_divider = '/'

        # initialize by loading ndjson file
        self.resource_name = self.extract_resource_name()
        self.df = None

    @property
    def number_of_calls(self):
        return len(self.counter_ids)

    def as_resource(self, jdict):
        if self._json_search_keyname not in jdict:
            return jdict
        if self._resource_type_divider not in jdict[self._json_search_keyname]:
            return jdict

        full_ref = jdict[self._json_search_keyname].split(self._resource_type_divider)
        resource_type = full_ref[0]
        id = full_ref[1]
        return Resource(id=id, resource_type=resource_type)

    def load_data(self):
        f = open(self.directory + os.sep + self.filename, 'r')
        lines = f.readlines()
        f.close()

        if self.resource_name == 'Patient':
            print('haha')

        resource_list = []
        for l in lines:
            ddict = loads(l, object_hook=self.as_resource)

            resources = Resource.__instances__
            for r in resources:
                resource_list.append([ddict['id'], r.resource_type, r.id])

            Resource.__instances__ = list()

        if resource_list:
            columns = ['id', self._json_search_keyname,
                       self._json_search_keyname + '_id']
            self.df = pd.DataFrame(resource_list, columns=columns)

    def extract_resource_name(self):
        return self.filename.replace(self._extension, '')

class PatientResourceDataObj(RescourceDataObj):
    # I want to have a specialized patient object that is a little clunky for intializing patient
    # but since I'm only using these functions with Patient made sense to split
    def __init__(self, *args, **kwargs):
        super(PatientResourceDataObj, self).__init__(*args, **kwargs)

        self._json_search_keynames = ['family', 'given']
        self._name_col = 'name'

    def load_data(self):
        f = open(self.directory + os.sep + self.filename, 'r')
        lines = f.readlines()
        f.close()

        patient_list = []
        for l in lines:
            ddict = loads(l, object_hook=self.as_patient)

            pat_insts = PatientResource.__instances__
            for r in pat_insts:
                patient_list.append([ddict['id'], r.firstname, r.lastname])

            PatientResource.__instances__ = list()

        if patient_list:
            columns = ['id', 'firstname', 'lastname']
            self.df = pd.DataFrame(patient_list, columns=columns)

    def as_patient(self, jdict):
        if all([kn in jdict.keys() for kn in self._json_search_keynames]):
            lastname = jdict['family']
            firstname = jdict['given'][0]
            return PatientResource(firstname=firstname, lastname=lastname)
        else:
            return jdict

    def set_patient_id(self, firstname, lastname, id):
        if id is None:
            id = self.find_patient_id(firstname, lastname)
        self.input_id = id

    def find_patient_id(self, firstname, lastname):
        patid = self.df[(self.df['firstname'] == firstname) & (
                self.df['lastname'] == lastname)].id.values
        assert len(patid) == 1, "You have two patients with the same name!!"
        return patid[0]

def load_all_data(data_directory='data'):
    full_data = dict()

    files = os.listdir(data_directory)
    for filename in files:
        if 'Patient' in filename:
            rdo = PatientResourceDataObj(filename)
        else:
            rdo = RescourceDataObj(filename)

        rdo.load_data()
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

        # not going to bother looking for the key cause I already did
        # and I know there's only one
        if resource == patient_key:
            rdo.counter_ids.append([rdo.input_id])
            continue

        if rdo.df is None:
            continue

        keyname = rdo._json_search_keyname
        # get all the patient info for this resource type if there is any
        df_pats = rdo.df[rdo.df[keyname] == data[patient_key].resource_name]
        if df_pats is not None:
            # get the ids for this resource type if it matches the patient id
            # and if this id hasn't already been accounted for
            df_pat = df_pats[(df_pats[keyname + '_id'] == id) & (~df_pats.id.isin(rdo.counter_ids))]
            rdo.counter_ids.extend(list(df_pat.id.unique()))

            ref_resources = rdo.df[keyname].unique()
            for ref_rs in ref_resources:
                if patient_key == ref_rs:
                    continue
                df_ref = rdo.df[rdo.df[keyname] == ref_rs]
                ref_ids_df = df_ref[(df_ref.id.isin(df_pat.id)) & (
                    ~df_ref[keyname + '_id'].isin(data[ref_rs].counter_ids))]
                ref_ids = ref_ids_df[keyname + '_id'].unique()
                data[ref_rs].counter_ids.extend(list(ref_ids))

        # I thought this might have to be recursive but
        # because every thing is linked you can go both ways
        # and get the same answer

    counter_table = [(resource, data[resource].number_of_calls) for resource in
                     sorted(data.keys(), key=lambda x: data[x].number_of_calls, reverse=True)]

    if verbose:
        print(tabulate(counter_table, headers=['Resource Type', 'Count']))

    return counter_table

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
        resource_counter_cli(firstname=None, lastname=None, id=arg_dict['id'])



if __name__ == "__main__":
    # command_line_arg_run(sys.argv)

    resource_counter_cli(firstname="Cleo27", lastname="Bode78", id=None)

    # f = open('data/DocumentReference.ndjson')
    # lines = f.readlines()
    # ddict = loads(lines[0], object_hook=as_resource)
    # print(Resource.__instances__)
    # print('haha')
