### Define schema for each CSV
from collections import namedtuple
from itertools import islice
from datetime import datetime
import csv

employement_tuple = namedtuple("Employment_Details",['employer','department','employee_id','ssn'])
personal_info_tuple = namedtuple("Personal_Info",['ssn','first_name','last_name','gender','language'])
update_status_tuple = namedtuple("Update_Status",['ssn','last_updated','created'])
vehicles_tuple = namedtuple("Vehicle_Details",['ssn','vehicle_make','vehicle_model','model_year'])
all_details_tuple = namedtuple("Full_Details",['ssn','fname', 'lname', 'gender', 'lang','employer', 'dept', 'emp_id','last_upd', 'created','vmake','vmodel','model_year'])
datetime_fields = {'model_year':"%Y", "last_updated":"%Y-%m-%dT%H:%M:%SZ", "created":"%Y-%m-%dT%H:%M:%SZ" }


def convert_datatypes(splitted_list, tuple_type):
    '''
    Converts a splitted array into respective namedtuple type.
    We have only 3 non-string columns i.e last_updated, created and vehicle's model_year
    '''
    if len(tuple_type._fields) != len(splitted_list):
        print(f"Input list doesn't match the tuple type {tuple_type}")
        raise TypeError

    return  (datetime.strptime(val, datetime_fields[t_type]) if(t_type in datetime_fields) else str(val) for val,t_type in zip(splitted_list, tuple_type._fields))


def lazy_file_reader(file_name, mode='r',skip_header=True):
    '''
    Reads a CSV file and creates a generator by yielding. If skip_header is set then we ignore the title/header line.
    By default all the current CSVs have the header hence it is always set to True.
    '''
    
    with open(file_name, mode, encoding='utf8') as f:
        if skip_header:
            next(f)
        try:
            yield from csv.reader(f, delimiter=',', quotechar='"')
        except Exception as e:
            print("Parsing Exception at:",e)

def file_specific_iterator(generator_type, tuple_type, max_count=None):
    '''
    For a given islice (based on max_count), this function will return the datatype corrected namedtuple based on tuple_type.
    '''
    for row in islice(generator_type, max_count):
        try:
            yield tuple_type(*convert_datatypes(row, tuple_type))
        except ValueError:
            print("Error for:",row)
            pass


class FullDbCreator:
    '''
    Iterable that takes the filenames as input and creates generators from each file 
    and returns one row of the merged file. The file_name_list must follow the below order:
    1. vehicles.csv
    2. updates.csv
    3. Employment.csv
    4. personal_info.csv
    '''
    def __init__(self, file_name_list, max_count=None, max_recorded_date=None):
        self.__file_name_list = file_name_list
        self.__max_count = max_count
        try:
            if max_recorded_date is not None:
                self.__max_recorded_date = datetime.strptime(max_recorded_date,'%d/%m/%Y')
            else:
                self.__max_recorded_date = None
        except Exception as e:
            print('Invalid date format')
            raise e           
    
    def __iter__(self):
        '''
        Iterator to call static generator function that aggregates and creates the final representation format.
        '''
        return FullDbCreator.file_specific_iterator(self.__file_name_list, self.__max_count, self.__max_recorded_date)

    @staticmethod
    def file_specific_iterator(file_name_list, max_count=None, max_recorded_date=None):
        '''
        Adapter styled function to invoke individual iterators for each file type.
        The iterators are invoked only once, however the yield is called in loop till StopIteration.
        After extracting the values from each file's iterator, we do the following:
        1. Check if SSN's match across iterators.
        2. If max_recorded_date is set then only such records whose last_updated date is more than max_recorded_date are selected 
        '''
        vehicle_val = file_specific_iterator(lazy_file_reader(file_name_list[0]), vehicles_tuple, max_count)
        update_val = file_specific_iterator(lazy_file_reader(file_name_list[1]), update_status_tuple, max_count)
        emp_val = file_specific_iterator(lazy_file_reader(file_name_list[2]), employement_tuple, max_count)
        personal_val = file_specific_iterator(lazy_file_reader(file_name_list[3]), personal_info_tuple, max_count)
        while True:
            try:
                #### Hopelessly long line. Potential PEP abuse.
                ssn,fname, lname, gender, lang,employer, dept, emp_id, emp_ssn, pers_ssn,last_upd, created, v_ssn,vmake,vmodel,model_year = *next(personal_val),*next(emp_val),*next(update_val),*next(vehicle_val)
                if(len(set([ssn,emp_ssn,pers_ssn,v_ssn])) > 1 ):
                    print("Mismatched SSN", ssn,emp_ssn,pers_ssn,v_ssn)
                    raise StopIteration
                if (max_recorded_date is not None and last_upd < max_recorded_date):
                    continue                    
                yield all_details_tuple(ssn,fname, lname, gender, lang,employer, dept, emp_id, last_upd, created,vmake,vmodel,model_year)
            except StopIteration:
                break


def get_most_pref_carmake_genderwise(full_table: 'List'):
    '''
    Returns the most popular models for each gender.
    Algorithm:
    For each gender type:
    1. Sort the gender specific entries based on car's make
    2. Create a map of Make vs Count
    3. Sort the map based on count in descending order
    4. Select all the models that have highest score.
    '''
    result_dict = {}
    for gender_val in set(map(lambda x: x.gender, full_table)):

        genderwise_list = sorted(list(filter(lambda x: x.gender == gender_val, full_table)), key=lambda x:x.vmake)
        model_list = [x.vmake for x in genderwise_list]

        x = { i: model_list.count(i) for i in set(model_list)}
        y = list(sorted(x.items(), key=lambda item: item[1], reverse=True))
        selected_list = list(filter(lambda x: x[1] >= y[0][1], y))
        result_dict[gender_val] =selected_list# (y[0], y[1])
    return result_dict

