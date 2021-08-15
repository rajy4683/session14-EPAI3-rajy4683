# Generators and Context Managers

The goal of this repo is evaluate the use of generators (and optionally context managers) to handle multiple files as described below:

```
personal_info.csv - personal information such as name, gender, etc. (one row per person)
vehicles.csv - what vehicle people own (one row per person)
employment.csv - where a person is employed (one row per person)
update_status.csv - when the person's data was created and last updated
```

Each file contains a key, SSN, which uniquely identifies a person. This key is present in all four files.

We are guaranteed that the same SSN value is present in every file, and that it only appears once per file.

In addition, the files are all sorted by SSN, i.e. the SSN values appear in the same order in each file.

Implemented code can be found in this [Colab Link]()

### Goal 1
Your first task is to create iterators for each of the four files that contained cleaned up data, of the correct type (e.g. string, int, date, etc), and represented by a named tuple. For now these four iterators are just separate, independent iterators.

#### Solution

We pre-define the following namedtuples. 

```
employement_tuple = namedtuple("Employment_Details",['employer','department','employee_id','ssn'])
personal_info_tuple = namedtuple("Personal_Info",['ssn','first_name','last_name','gender','language'])
update_status_tuple = namedtuple("Update_Status",['ssn','last_updated','created'])
vehicles_tuple = namedtuple("Vehicle_Details",['ssn','vehicle_make','vehicle_model','model_year'])
all_details_tuple = namedtuple("Full_Details",['ssn','fname', 'lname', 'gender', 'lang','employer', 'dept', 'emp_id','last_upd', 'created','vmake','vmodel','model_year']) 
```

We create two generic functions as below:

```
def lazy_file_reader(file_name, mode='r',skip_header=True):
    '''
    A generator function to read and yield one line from a file indicated by "file_name"
    '''
def file_specific_iterator(generator_type, tuple_type, max_count=None):
    '''
    Creates a slice of a given generator_type and yields namedtuple based on tuple_type.
    '''
def convert_datatypes(splitted_list, tuple_type):
    '''
    Converts a splitted array into respective namedtuple type.
    We have only 3 non-string columns i.e last_updated, created and vehicle's model_year
    '''
```



### Goal 2
Create a single iterable that combines all the columns from all the iterators. The iterable should yield named tuples containing all the columns. Make sure that the SSN's across the files match! All the files are guaranteed to be in SSN sort order, and every SSN is unique, and every SSN appears in every file. Make sure the SSN is not repeated 4 times - one time per row is enough!

#### Solution

We create an iterable as below:

```
class FullDbCreator:
    '''
    Iterable that takes the filenames as input and creates generators from each file 
    and returns one row of the merged file
    '''
    def __init__(self, file_name_list, max_count=None, max_recorded_date=None):
        '''
        Init function to create class object.
        '''
    def __iter__(self):
        '''
        Iterator to call static generator function that aggregates and returns one full row of the dataset.
        '''
```

The `__iter__()` method invokes a static method in the class as below:

```
    @staticmethod
    def file_specific_iterator(file_name_list, max_count=None, max_recorded_date=None):
        '''
        Adapter styled function to invoke individual iterators for each file type.
        The iterators are invoked only once, however the yield is called in loop till StopIteration.
        After extracting the values from each file's iterator, we do the following:
        1. Check if SSN's match across iterators.
        2. If max_recorded_date is set then only such records whose last_updated date is more than max_recorded_date are selected 
        '''
```



### Goal 3
Next, we want to identify any stale records, where stale simply means the record has not been updated since 3/1/2017 (e.g. last update date < 3/1/2017). Create an iterator that only contains current records (i.e. not stale) based on the last_updated field from the status_update file.

### Solution

The `FullDbCreator` class above accepts an optional parameter `max_recorded_date` which must be a date string of format `'%d/%m/%Y'`

Sample code as below:

```
new_full_db = FullDbCreator(file_list, max_recorded_date='20/3/2018')
```

### Goal 4
Find the largest group of car makes for each gender. Possibly more than one such group per gender exists (equal sizes).

#### Solution:

Below function implements this requirement. We pass a list of all the required entries.

```
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
```

## User Details:

Submitted by: Rajesh Y(github: rajy4683)
Email ID: st.hazard@gmail.com
