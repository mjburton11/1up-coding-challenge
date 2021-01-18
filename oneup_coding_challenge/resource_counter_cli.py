import pandas as pd
import os
from tabulate import tabulate
from json import loads
import sys

class InstanceRegister:
    """
    This method is used as a way collect resources as they
    are discovered when reading the json data

    By forcing dicts that meet a certain criteria when reading
    the json to be a Resource or PatientResource object I can
    append the their newly created objects by using this method
    so that I don't have to parse decoded json looking for
    these resources
    """
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
    def __init__(self, patient_id, directory='data', ignore_file='Patient.ndjson'):
        self.patient_id = patient_id
        self.directory = directory

        # counter id will store how many unique times this object
        # is associated with a patient
        self.counter_ids = {'Patient': [patient_id]}
        self._extension = '.ndjson'
        self.ignore_file = ignore_file

        # when decoding the json, look for these keys
        # to determine if it is a resource or not
        self._json_search_keyname = 'reference'
        self._resource_type_divider = '/'

    def count_resources(self):
        return [(resource, len(self.counter_ids[resource])) for resource in
                 sorted(self.counter_ids.keys(), key=lambda x: len(self.counter_ids[x]), reverse=True)]

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
        files = os.listdir(self.directory)
        for filename in files:
            # ignore patient because I know there's only one
            if self.ignore_file in filename:
                continue

            f = open(self.directory + os.sep + filename, 'r')
            lines = f.readlines()
            f.close()

            resource_name = self.extract_resource_name(filename)
            if resource_name not in self.counter_ids:
                self.counter_ids[resource_name] = []

            # for each line in the ndjson I will look for any
            # resources that are created using object_hook
            # then I will append only the ids of the resources
            # that have not already been accounted for
            for l in lines:
                ddict = loads(l, object_hook=self.as_resource)

                resources = Resource.__instances__
                if self.patient_id in [r.id for r in resources]:
                    # first append the top level id
                    if ddict['id'] not in self.counter_ids[resource_name]:
                        self.counter_ids[resource_name].append(ddict['id'])
                    # then look for any other resources that were created
                    # with this associated patient and append them to the
                    # counter lists
                    for r in resources:
                        if r.resource_type != 'Patient':
                            if r.resource_type not in self.counter_ids:
                                self.counter_ids[r.resource_type] = [r.id]
                            elif r.id not in self.counter_ids[r.resource_type]:
                                self.counter_ids[r.resource_type].append(r.id)

                # reset for next line
                Resource.__instances__ = list()

    def extract_resource_name(self, filename):
        return filename.replace(self._extension, '')

class PatientResourceDataObj(object):
    # I want to have a specialized patient object that is a little clunky for intializing patient
    # but since I'm only using these functions with Patient made sense to split
    def __init__(self, firstname, lastname, id, directory='data'):

        self.firstname = firstname
        self.lastname = lastname
        self.id = id
        self.check_bad_inputs()

        self.directory = directory
        self.find_by_id = self.id_or_name()

        self._filename = 'Patient.ndjson'
        self._json_search_keynames = ['family', 'given']
        self._name_col = 'name'

    def check_bad_inputs(self):

        if all([v is not None for v in [self.firstname, self.lastname, self.id]]):
            raise ValueError('Must specify either names or self.id')
        elif all([v is None for v in [self.firstname, self.lastname, self.id]]):
            raise ValueError('Must specify either names or self.id')
        elif self.firstname is not None and self.lastname is None:
            raise ValueError('Must specify first and last name')
        elif self.firstname is None and self.lastname is not None:
            raise ValueError('Must specify first and last name')
        elif self.firstname is not None and self.id is not None:
            raise ValueError('Must specify either names or self.id')
        elif self.lastname is not None and self.id is not None:
            raise ValueError('Must specify either names or id')

    def id_or_name(self):
        if self.firstname is not None and self.lastname is not None:
            self.find_by_id = False
        else:
            self.find_by_id = True

    def load_data(self):
        f = open(self.directory + os.sep + self._filename, 'r')
        lines = f.readlines()
        f.close()

        for l in lines:
            ddict = loads(l, object_hook=self.as_patient)

            # this assumes that their is only one patient with this
            # name combination and associated id
            pat_insts = PatientResource.__instances__
            for r in pat_insts:
                if r.firstname == self.firstname and r.lastname == self.lastname:
                    self.id = ddict['id']
                    break

            if self.id is not None:
                break

            PatientResource.__instances__ = list()

        if self.id is None:
            raise ValueError('didnt find name of patient')

    def as_patient(self, jdict):
        if all([kn in jdict.keys() for kn in self._json_search_keynames]):
            lastname = jdict['family']
            firstname = jdict['given'][0]
            return PatientResource(firstname=firstname, lastname=lastname)
        else:
            return jdict

def resource_counter_cli(firstname, lastname, id, verbose=True):

    # get the patient id first
    pat = PatientResourceDataObj(firstname, lastname, id)
    if not pat.find_by_id:
        pat.load_data()
        id = pat.id

    # load the data and build up the counter as you parse the json
    rdo = RescourceDataObj(id)
    rdo.load_data()
    counter_table = rdo.count_resources()

    if verbose:
        print(tabulate(counter_table, headers=['Resource Type', 'Count']))

    return counter_table

def command_line_arg_run(sys_args):
    # this is odd length because first argment is the filename
    if len(sys_args) % 2 != 1:
        raise ValueError('You must specify an argument with each keyword!')

    arg_dict = {'firstname': None,
                'lastname': None,
                'id': None}

    for i, sa in enumerate(sys_args):
        inp = sa.replace('-', '')
        if inp in arg_dict.keys():
            arg_dict[inp] = sys_args[i+1]

    resource_counter_cli(firstname=arg_dict['firstname'], lastname=arg_dict['lastname'],
                         id=arg_dict['id'])

if __name__ == "__main__":
    command_line_arg_run(sys.argv)

    # resource_counter_cli(firstname="Cleo27", lastname="Bode78", id=None)

