from __future__ import absolute_import
from __future__ import annotations
from __future__ import division
from __future__ import print_function
from itertools import islice

import sys
import csv
import logging
from typing import List, Tuple
import uuid
import numpy as np
import pandas as pd


# Note (john): Make sure you use Python's logger to log
#              information about your program
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

# Generates unique operator IDs
def _generate_uuid():
    return uuid.uuid4()


# Custom tuple class with optional metadata
class ATuple:
    """Custom tuple.

    Attributes:
        tuple (Tuple): The actual tuple.
        metadata (string): The tuple metadata (e.g. provenance annotations).
        operator (Operator): A handle to the operator that produced the tuple.
    """
    def __init__(self, tuple, metadata=None, operator=None):
        self.tuple = tuple
        self.metadata = metadata
        self.operator = operator
        self.where_prov = None
        self.how_prop = ""
        self.res = []

    # Returns the lineage of self
    def lineage(self) -> List[ATuple]:
        # def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        return self.operator.lineage(self,tuples=[self])

    # Returns the Where-provenance of the attribute at index 'att_index' of self
    def where(self,att_index=0) -> List[Tuple]:
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        return self.operator.where(self,att_index=0,tuples = [self])

    # Returns the How-provenance of self
    def how(self) -> string:
        # YOUR CODE HERE (ONLY FOR TASK 3 IN ASSIGNMENT 2)
        return self.operator.how(self,tuples=[self])

    # Returns the input tuples with responsibility \rho >= 0.5 (if any)
    def responsible_inputs(self) -> List[ATuple]:
        # YOUR CODE HERE (ONLY FOR TASK 4 IN ASSIGNMENT 2)
        return self.operator.responsible_inputs(self,tuples=[self])
    
    # pravite function __repr__
    def __repr__(self):
        return str(self.tuple)

# Data operator
class Operator:
    """Data operator (parent class).

    Attributes:
        id (string): Unique operator ID.
        name (string): Operator name.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    def __init__(self, id=None, name=None, track_prov=False,
                                           propagate_prov=False):
        self.id = _generate_uuid() if id is None else id
        self.name = "Undefined" if name is None else name
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov
        logger.debug("Created {} operator with id {}".format(self.name,
                                                             self.id))

    # NOTE (john): Must be implemented by the subclasses
    def get_next(self):
        logger.error("Method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def lineage(self, tuples: List[ATuple]) -> List[List[ATuple]]:
        logger.error("Lineage method not implemented!")

    # NOTE (john): Must be implemented by the subclasses
    def where(self, att_index: int, tuples: List[ATuple]) -> List[List[Tuple]]:
        logger.error("Where-provenance method not implemented!")

    def how(self, tuples: List[ATuple]):
        logger.error("how method not implemented!")

    def responsible_inputs(self, tuples):
        logger.error("responsible_inputs method not implemented!")

# Scan operator
class Scan(Operator):
    """Scan operator.

    Attributes:
        filepath (string): The path to the input file.
        filter (function): An optional user-defined filter.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes scan operator
    def __init__(self, filepath, batch, filter=None,track_prov=False,
                                              propagate_prov=False):
        super(Scan,self).__init__(name="Scan", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.filepath = filepath
        self.filter = filter
        self.batch = batch
        # start from last place
        self.start = 0        
        self.scanfile = []
        with open(self.filepath, newline = '' ) as f:
            # use a list to store whole doc
            filelist = list(tuple(line) for line in csv.reader(f,delimiter=' '))
        for singlelist in filelist:
            # give value to metadata
            if(track_prov == True):
                # metadata --> list
                singletuple = ATuple(tuple = singlelist, metadata = [singlelist], operator = Scan)
                # list-->lineagelist-->[[tuples.metadata][..][..]] = [[singlelist],[singlelist],[singlelist],[singlelist]]
                singletuple.metadata = singletuple
            else:
                singletuple = ATuple(tuple = singlelist, metadata=None, operator = Scan)
            # local variable 'singletuple' referenced before assignment
            self.scanfile.append(singletuple)
        if(track_prov == True):
            for line in range(len(self.scanfile)):
                k = self.scanfile[line]
                new_tuple = (filepath,line+1,self.scanfile[line],self.scanfile[line].tuple[-1])
                k.where_prov = new_tuple
        if(propagate_prov == True):
            for line in range(len(self.scanfile)):
                k = self.scanfile[line]
                if filepath == 'data/f_t.txt' or filepath == 'data/friends.txt' or filepath == 'data/toyfriends.txt':
                # (f1)
                    fi = "f"+str(k.where_prov[1])
                    k.how_prop = fi
                elif filepath == 'data/m_t.txt' or filepath == 'data/movie_ratings.txt' or filepath == 'data/toymovie.txt':
                    fi = "r"+str(k.where_prov[1])
                    k.how_prop = fi
                else:
                    fi = "t"+str(k.where_prov[1])
                    k.how_prop = fi
            # for each row, the where provenance --> ('Ratings.csv',4,(1.10,5),5)----can't get the last 5?

    # Returns next batch of tuples in given file (or None if file exhausted)
    def get_next(self):
        # YOUR CODE HERE
        if ((len(self.scanfile) - self.batch * self.start) > self.batch):
            self.start = self.start + 1
            # fix assignment 1 problem: return list but not Atuple.
            return self.scanfile[self.batch * (self.start - 1) : self.batch * self.start]
        # batch size is bigger than the file line
        elif ((len(self.scanfile) - self.batch * self.start) > 0 and (len(self.scanfile) - self.batch * self.start) < self.batch):
            self.start = self.start + 1
            return self.scanfile[self.batch * (self.start - 1) : ]
        else:
            # (or None if file exhausted)
            return None

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        # list output
        scan_lineage_output = []
        for row in tuples:
            scan_lineage_output.append(row.metadata)
        return scan_lineage_output

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        scan_lineage_input = self.lineage()
        # att_index = 2
        where_output = []
        # [('2', '3', '4')]
        # len = 3
        # index = 3 --->4th
        for row in scan_lineage_input:
            # ('2', '3', '4')
            # index = 2
            # add '4' at the end of tuple
            where_tuple = []
            # (('2', '3', '4'),4)
            where_tuple.append(row.where_prov)
                # [('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))]
            where_output.append(where_tuple)
            # [[('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))],[('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))]]
        return where_output
    
    # SCAN((f1),(f2),(f3),(f4))
    def how(self, tuples):
        for row in range(len(tuples)):
            string = 'Scan( '
            if row == len(tuples)-1:
                string = string + "("+str(tuples[row].how_prop)+")"
            else:
                string = string + "("+str(tuples[row].how_prop)+")" + ', '
        string = string + ' )'
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

    def responsible_inputs(self, tuples):
        output = []
        for i in tuples:
            output = output + i.res
            return output

class Join(Operator):
#     """Equi-join operator.

#     Attributes:
#         left_input (Operator): A handle to the left input.
#         right_input (Operator): A handle to the left input.
#         left_join_attribute (int): The index of the left join attribute.
#         right_join_attribute (int): The index of the right join attribute.
#         track_prov (bool): Defines whether to keep input-to-output
#         mappings (True) or not (False).
#         propagate_prov (bool): Defines whether to propagate provenance
#         annotations (True) or not (False).
#     """
#     # Initializes join operator
    def __init__(self, batch, left_input, right_input, left_join_attribute,
                                                right_join_attribute,
                                                track_prov=False,
                                                propagate_prov=False):
        super(Join,self).__init__(name="Join", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        # use dictionary to join
        left_input_alllist = []
        left_input_batch = left_input.get_next()
        while(left_input_batch):
            left_input_alllist = left_input_alllist + left_input_batch
            # nextbatch
            left_input_batch = left_input.get_next()
        self.dict_left = {}
        # use dict to hash join
        for row in left_input_alllist:
            # dict.setdefault(key, default=None)
            self.dict_left.setdefault(row.tuple[left_join_attribute],[]).append(row)
            #self.dict_left: {'2': [Atuple...], '3': [Atuple...], '4': [Atuple...], '5': [Atuple...]}
        self.right_input = right_input
        self.right_join_attribute = right_join_attribute
        self.right_input_batch = self.right_input.get_next()
        self.start = 0
        self.batch = batch
        self.join_output_list = []
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov

    # Returns next batch of joined tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        if(self.right_input_batch != None):
            for right_row in self.right_input_batch:
                for left_key in self.dict_left.keys():
                    if(right_row.tuple[self.right_join_attribute] == left_key):
                        # right_join_attribute = left_join_attribute
                        for left_row in self.dict_left[left_key]:
                            join_tuple_row = left_row.tuple + right_row.tuple
                            # print(join_tuple_row)
                            if (self.track_prov == True):
                                join_Atuple_row = ATuple(join_tuple_row,metadata=[left_row,right_row], operator=Join)
                                join_Atuple_row.where_prov = [left_row.where_prov,right_row.where_prov]
                            else:
                                join_Atuple_row = ATuple(join_tuple_row,metadata=None, operator=Join)
                                # print(join_Atuple_row)
                            if(self.propagate_prov == True):
                                join_Atuple_row.how_prop = "("+left_row.how_prop+"*"+right_row.how_prop+"@"+str(right_row.where_prov[2].tuple[-1])+")"
                            self.join_output_list.append(join_Atuple_row)
            self.right_input_batch = self.right_input.get_next()
            # join's output
            self.join_output = self.join_output_list
            # length = len(self.join_output)
            if(self.join_output):
                # return self.join_output
                if ((len(self.join_output) - self.batch * self.start) > self.batch):
                    self.start = self.start + 1
                    return self.join_output[self.batch * (self.start - 1) : self.batch * self.start]
        # batch size is bigger than the file line
                elif ((len(self.join_output) - self.batch * self.start) > 0 ):
                    self.start = self.start + 1
                    return self.join_output[self.batch * (self.start - 1) : ]
            elif(self.join_output == None and self.right_input_batch != None):
                return self.get_next()
        else:
            return None

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        join_lineage_output = []
        for row in tuples:
            # [[leftlist],[rightlist]]
            join_lineage_output.append(row.metadata)
        return join_lineage_output

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        join_lineage_input = self.lineage()
        where_output = []
        # [[[('1', '2'), ('2', '3', '4')]
        # index = 2
        for row in join_lineage_input:
            where_list = []
            where_list.append(row[0].where_prov)
            where_list.append(row[1].where_prov)
            where_output.append(where_list)
        return where_output
    
    # Join( (f1*r3@5), (f1*r3@5), (f1*r3@5), (f1*r3@5)... )
    def how(self, tuples):
        for row in range(len(tuples)):
            string = 'Join( '
            if row == len(tuples)-1:
                string = string +str(tuples[row].how_prop)
            else:
                string = string +str(tuples[row].how_prop) + ', '
        string = string + ' )'
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

# Project operator
class Project(Operator):
    """Project operator.

    Attributes:
        input (Operator): A handle to the input.
        fields_to_keep (List(int)): A list of attribute indices to keep.
        If empty, the project operator behaves like an identity map, i.e., it
        produces and output that is identical to its input.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes project operator
    def __init__(self, batch, input, fields_to_keep=[], track_prov=False,
                                                 propagate_prov=False):
        super(Project,self).__init__(name="Project", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.fields_to_keep = fields_to_keep
        self.batch = batch
        self.start = 0
        self.project_output = []
        self.project_input_batch = self.input.get_next()
        self.track_prov = track_prov
        self.propagate_prov = propagate_prov

    # Return next batch of projected tuples (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        if(self.project_input_batch == None):
            return None
        else:
            if(self.fields_to_keep == None):
                self.project_output = self.project_output.append(self.project_input_batch)
            else:
                for rows in self.project_input_batch:
                    project_tuple_output = ()
                    for field in self.fields_to_keep:
                        project_tuple_output = project_tuple_output + (rows.tuple[field],)
                    # direct rewrite value for every row in input batch
                    rows.tuple = project_tuple_output
                    if (self.track_prov == True):
                        # rows' metadata == same metadata
                        rows.operator = Project
                self.project_output = self.project_output + self.project_input_batch
            self.project_input_batch = self.input.get_next()  
            length = len(self.project_output)
            if ((length - self.batch * self.start) > self.batch):
                self.start = self.start + 1
                return self.project_output[self.batch * (self.start - 1) : self.batch * self.start]
        # batch size is bigger than the file line
            elif ((length - self.batch * self.start) > 0):
                self.start = self.start + 1
                return self.project_output[self.batch * (self.start - 1) : ]
            else:
                return self.get_next()
    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        project_lineage_output = []
        for row in tuples:
            project_lineage_output.append(row.metadata)
        return project_lineage_output

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        scan_lineage_input = self.lineage()
        # att_index = 2
        where_output = []
        # [('2', '3', '4')]
        # len = 3
        # index = 3 --->4th
        for row in scan_lineage_input:
            # ('2', '3', '4')
            # index = 2
            # add '4' at the end of tuple
            where_tuple = []
            # (('2', '3', '4'),4)
            if(type(row)==list):
                for row1 in row:
                    if(len(row)>0):
                        where_tuple.append(row1[1].where_prov)
            else:
                where_tuple.append(row.where_prov)
                # [('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))]
            where_output.append(where_tuple)
            # [[('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))],[('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))]]
        return where_output
    
    def how(self, tuples):
        for row in range(len(tuples)):
            string = 'Project( '
            if row == len(tuples)-1:
                string = string +str(tuples[row].how_prop)
            else:
                string = string + str(tuples[row].how_prop)+ ', '
        string = string + ' )'
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

     
# Group-by operator
class GroupBy(Operator):
    """Group-by operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples.
        value (int): The index of the attribute we want to aggregate.
        agg_fun (function): The aggregation function (e.g. AVG)
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes average operator
    def __init__(self,batch, input, key, value, agg_fun, track_prov=False,
                                                   propagate_prov=False):
        super(GroupBy,self).__init__(name="GroupBy", track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.key = key
        self.value = value
        self.agg_fun = agg_fun
        self.track_prov = track_prov
        # get all groupby input
        groupby_input_batch = input.get_next()
        groupby_input = []
        while(groupby_input_batch):
            groupby_input = groupby_input + groupby_input_batch
            groupby_input_batch = input.get_next()
        # use dict to group by
        groupby_dict = {}
        for row in groupby_input:
            groupby_dict.setdefault(row.tuple[key],[]).append(row)
        groupby_output = []

        if(groupby_input):
            # key --> rating
            # value --> index
            if (value > len(groupby_input[0].tuple)):
                print("Can't groupby")
                sys.exit(0)
            else:
                for temp_key in groupby_dict.keys():
                    temp_key_list = []
                    metadata_list_key = []
                    groupby_output_tuple = ()
                    for row in groupby_dict[temp_key]:
                        # movie 3's ratings
                        temp_key_list.append(row)
                        metadata_list_key.append(row.metadata)
                    avg = agg_fun(self.value, temp_key_list)
                    groupby_output_tuple = (temp_key,avg)
                    if(track_prov == True):
                            # for every row in groupby_dict, metadata is all key's metadata
                            # [[row[0].metadata],[row[1].metadata],[row[2].metadata]...]
                        newtuple = ATuple(tuple = groupby_output_tuple, metadata = metadata_list_key, operator=GroupBy)
                        if(propagate_prov == True):
                            howlist = ''
                            for i in temp_key_list:
                                if(i == temp_key_list[-1]):
                                    howlist=howlist+i.how_prop
                                # elif (i==temp_key_list[0]):
                                #     howlist=howlist+i.how_prop+", "
                                else:
                                    howlist=howlist+i.how_prop+", "
                            newtuple.how_prop = howlist
                        groupby_output.append(newtuple)  
                    else:
                        groupby_output.append(ATuple(tuple = groupby_output_tuple, metadata=None, operator=None))  

        else:
            print("Can't groupby")
            sys.exit(0)
        self.groupby_output = groupby_output
        self.batch = batch
        self.start = 0


    # Returns aggregated value per distinct key in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        length = len(self.groupby_output)
        if ((length - self.batch * self.start) > self.batch):
            self.start = self.start + 1
            return self.groupby_output[self.batch * (self.start - 1) : self.batch * self.start]
    # batch size is bigger than the file line
        elif ((length - self.batch * self.start) > 0):
            self.start = self.start + 1
            return self.groupby_output[self.batch * (self.start - 1) : ]
        else:
            return None     
    
    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        groupby_lineage_output = []
        for row in tuples:
            groupby_lineage_output.append(row.metadata)
        return groupby_lineage_output

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        groupby_lineage_input = self.lineage()
        # [[[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]]
        where_output = []
        # tuples = [(4.0,)] --> output
        for row in groupby_lineage_input:
            where_list = []
            # [('1', '2'), ('2', '3', '4')]
            for row_ in row:
                if(type(row_)==list):
                    row_[1].where_prov = row_[1].where_prov
                    where_list.append(row_[1].where_prov)
                else:
                    row_.where_prov = row_.where_prov
                    where_list.append(row_.where_prov)
            where_output.append(where_list)
        return where_output
    
    def how(self, tuples):
        for row in range(len(tuples)):
            string = 'GroupBy( '
            if row == len(tuples)-1:
                string = string + str(tuples[row].how_prop)
            else:
                string = string + str(tuples[row].how_prop) + ', '
        string = string + ' )'
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

    def responsible_inputs(self, tuples):
        output = []
        for i in tuples:
            output = output + i.res
            return output


# Custom histogram operator
class Histogram(Operator):
    """Histogram operator.

    Attributes:
        input (Operator): A handle to the input
        key (int): The index of the key to group tuples. The operator outputs
        the total number of tuples per distinct key.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes histogram operator
    def __init__(self, input, key=0, track_prov=False, propagate_prov=False):
        super(Histogram,self).__init__(name="Histogram",
                                        track_prov=track_prov,
                                        propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        #key = 1 2 3 4 5
        hist_input_batch = input.get_next()
        hist_input = []
        while(hist_input_batch):
            hist_input = hist_input + hist_input_batch
            hist_input_batch = input.get_next()
        dict_hist = {}
        for row in hist_input:
            dict_hist.setdefault(row.tuple[key],[]).append(row)
        hist_output = []
        for temp_key in dict_hist.keys():
            metadata_list = []
            for row in dict_hist[temp_key]:
                metadata_list.append(row.metadata)
            if(track_prov):
                hist_output_single = ATuple(tuple = (temp_key,len(dict_hist[temp_key])),metadata = metadata_list, operator = Histogram)
            else:
                hist_output_single = ATuple(tuple = (temp_key,len(dict_hist[temp_key])),metadata = None, operator = Histogram)
            hist_output.append(hist_output_single)
        self.hist_output = hist_output

    # Returns histogram (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        # list no ATuple object
        return self.hist_output
    
    def lineage(self,tuples):
        hist_lineage_output = []
        for row in tuples:
            hist_lineage_output.append(row.metadata)
        return hist_lineage_output
    
    def where(self,att_index,tuples):
        hist_lineage_output = self.lineage()
        # [[[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]]
        where_output = []
        # tuples = [(4.0,)] --> output
        for row in hist_lineage_output:
            where_list = []
            # [('1', '2'), ('2', '3', '4')]
            for row_ in row:
                row_[1].where_prov = row_[1].where_prov + (row_[1].tuple[-1],)
                where_list.append(row_[1].where_prov)
            where_output.append(where_list)
        return where_output
    
    def how(self, tuples):
        for row in range(len(tuples)):
            string = 'GroupBy( '
            if row == len(tuples)-1:
                string = string + "("+str(tuples[row].how_prop)+")"
            else:
                string = string + "("+str(tuples[row].how_prop)+")" + ', '
        string = string + ' )'
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

    def responsible_inputs(self, tuples):
        output = []
        for i in tuples:
            output = output + i.res
            return output

# Order by operator
class OrderBy(Operator):
    """OrderBy operator.

    Attributes:
        input (Operator): A handle to the input
        comparator (function): The user-defined comparator used for sorting the
        input tuples.
        ASC (bool): True if sorting in ascending order, False otherwise.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes order-by operator
    def __init__(self, batch, index, input, comparator, ASC=True, track_prov=False,
                                                    propagate_prov=False):
        super(OrderBy,self).__init__(name="OrderBy",
                                      track_prov=track_prov,
                                      propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.comparator = comparator
        self.ASC = ASC
        self.batch = batch
        orderby_input_batch = input.get_next()
        orderby_input = []
        while(orderby_input_batch):
            orderby_input = orderby_input + orderby_input_batch
            orderby_input_batch = input.get_next()
        self.orderby_output = comparator(ASC,orderby_input,index)
        # self.sechigh = sechigh
        for row in self.orderby_output:
            row.operator = OrderBy
            if(track_prov):
                row.metadata = row.metadata
        self.start = 0

    # Returns the sorted input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        length = len(self.orderby_output)
        if ((length - self.batch * self.start) > self.batch):
            self.start = self.start + 1
            return self.orderby_output[self.batch * (self.start - 1) : self.batch * self.start]
    # batch size is bigger than the file line
        elif ((length - self.batch * self.start) > 0):
            self.start = self.start + 1
            return self.orderby_output[self.batch * (self.start - 1) : ]
        else:
            return None  

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        orderby_lineage_output = []
        for row in tuples:
            orderby_lineage_output.append(row.metadata)
        return orderby_lineage_output

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        orderby_lineage_output = self.lineage()
        # [[[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]]
        where_output = []
        # tuples = [(4.0,)] --> output
        for row in orderby_lineage_output:
            where_list = []
            # [('1', '2'), ('2', '3', '4')]
            if(type(row)==list):
                for row_ in row:
                    row_[1].where_prov = row_[1].where_prov
                    where_list.append(row_[1].where_prov)
            else:
                row.where_prov = row.where_prov
                where_list.append(row.where_prov)
            where_output.append(where_list)
        return where_output

    def how(self, tuples):
        for row in range(len(tuples)):
            string = 'OrderBy( '
            if row == len(tuples)-1:
                string = string + str(tuples[row].how_prop)
            else:
                string = string + str(tuples[row].how_prop) + ', '
        string = string + ' )'
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

    def responsible_inputs(self, tuples):
        output = []
        for i in tuples:
            output = output + i.res
            return output

# Top-k operator
class TopK(Operator):
    """TopK operator.

    Attributes:
        input (Operator): A handle to the input.
        k (int): The maximum number of tuples to output.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes top-k operator
    def __init__(self, batch, input, k, track_prov=False, propagate_prov=False):
        super(TopK,self).__init__(name="TopK", track_prov=track_prov,
                                   propagate_prov=propagate_prov)
        # YOUR CODE HERE
        self.input = input
        self.k = k
        self.batch = batch
        self.start = 0
        global sechigh
        topk_input_batch = input.get_next()
        topk_input = []
        while(topk_input_batch):
            topk_input = topk_input + topk_input_batch
            topk_input_batch = input.get_next()
        topk_output_list = []
        if(len(topk_input) < k or k == 0 or k == None):
            self.topk_output = topk_input
        else:
            for i in range(k):
                topk_output_list.append(topk_input[i])
            self.topk_output = topk_output_list
        if(len(topk_input)<=k):
            sechigh = 0
        else:
            sechigh = topk_input[k]
        for row in self.topk_output:
            row.operator = TopK
            if track_prov:
                row.where_prov = row.where_prov

    # Returns the first k tuples in the input (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        # init the list of top k
        length = len(self.topk_output)
        if ((length - self.batch * self.start) > self.batch):
            self.start = self.start + 1
            return self.topk_output[self.batch * (self.start - 1) : self.batch * self.start]
    # batch size is bigger than the file line
        elif ((length - self.batch * self.start) > 0):
            self.start = self.start + 1
            return self.topk_output[self.batch * (self.start - 1) : ]
        else:
            return None  

    # Returns the lineage of the given tuples
    def lineage(self, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 1 IN ASSIGNMENT 2)
        topk_lineage_output = []
        for row in tuples:
            topk_lineage_output.append(row.metadata)
        return topk_lineage_output

    # Returns the where-provenance of the attribute
    # at index 'att_index' for each tuple in 'tuples'
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        topk_lineage_output = self.lineage()
        # [[[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]]
        where_output = []
        # tuples = [(4.0,)] --> output
        for row in topk_lineage_output:
            where_list = []
            # [('1', '2'), ('2', '3', '4')]
            for row_ in row:
                row_[1].where_prov = row_[1].where_prov
                where_list.append(row_[1].where_prov)
            where_output.append(where_list)
        return where_output
        
    # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]] -> ['3',4]
    # TOPk( (...), (...) )
    def how(self, tuples):
        for row in range(len(tuples)):
            string = 'TopK( '
            if row == len(tuples)-1:
                string = string + str(tuples[row].how_prop)
            else:
                string = string + str(tuples[row].how_prop) + ', '
        string = string + ' )'
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

    def responsible_inputs(self,tuples):
        sec = sechigh.tuple[1]
        new_tuple = []
        for row in self.lineage():
            for i in range(len(row)):
                if(topkcompare(sec,row,i)) == 1:
                    newtuple=(row[i][0],1)
                    newtuple2 = (row[i][1],1)
                    new_tuple.append(newtuple)
                    new_tuple.append(newtuple2)
                elif (topkcompare(sec,row,i)) == 2:
                    newtuple = (row[i][0],0.5)
                    newtuple2 = (row[i][1],0.5)
                    new_tuple.append(newtuple)
                    new_tuple.append(newtuple2)
        return new_tuple

# Filter operator
class Select(Operator):
    """Select operator.

    Attributes:
        input (Operator): A handle to the input.
        predicate (function): The selection predicate.
        track_prov (bool): Defines whether to keep input-to-output
        mappings (True) or not (False).
        propagate_prov (bool): Defines whether to propagate provenance
        annotations (True) or not (False).
    """
    # Initializes select operator
    def __init__(self, input, batch, id_index, name, predicate, track_prov=False,
                                         propagate_prov=False):
        super(Select,self).__init__(name="Select", track_prov=track_prov,
                                     propagate_prov=propagate_prov)
        # YOUR CODE HERE
        select_input = input.get_next() #list[Atuple,Atuple, ... ]
        # select scale is whole file
        # list
        all_input = []
        while(select_input):
            all_input = all_input + select_input
            # go on get_next
            select_input = input.get_next()
        # return list
        self.select_output = predicate(id_index, name, all_input)
        for row in self.select_output:
            row.operator = Select
            if(track_prov):
                row.metadata = row.metadata.metadata
            if(propagate_prov):
                row.how_prop = row.metadata.how_prop
        self.batch = batch
        self.start = 0
        self.id_index = id_index

    # Returns next batch of tuples that pass the filter (or None if done)
    def get_next(self):
        # YOUR CODE HERE
        #init select result list
        length = len(self.select_output)
        if ((length - self.batch * self.start) > self.batch):
            self.start = self.start + 1
            return self.select_output[self.batch * (self.start-1):self.batch * self.start]
        elif ((length - self.batch * self.start) > 0):
            self.start = self.start + 1
            return self.select_output[self.batch * (self.start-1):]
        else:
            return None
    
    def lineage(self, tuples):
        select_lineage_output = []
        for row in tuples:
            select_lineage_output.append(row.metadata)
        return select_lineage_output
    
    def where(self, att_index, tuples):
        # YOUR CODE HERE (ONLY FOR TASK 2 IN ASSIGNMENT 2)
        scan_lineage_input = self.lineage()
        # att_index = 2
        where_output = []
        # [('2', '3', '4')]
        # len = 3
        # index = 3 --->4th
        for row in scan_lineage_input:
            # ('2', '3', '4')
            # index = 2
            # add '4' at the end of tuple
            where_tuple = []
            # (('2', '3', '4'),4)
            where_tuple.append(row.where_prov)
                # [('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))]
            where_output.append(where_tuple)
            # [[('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))],[('file',4,('2', '4', '4')),('file',5,('2', '3', '4'))]]
        return where_output
    
    # SCAN((f1),(f2),(f3),(f4))
    def how(self, tuples):
        string = 'Select( '
        for row in range(len(tuples)):
            if row == len(tuples)-1:
                string = string + "("+str(tuples[row].how_prop)+")"
            else:
                string = string + "("+str(tuples[row].how_prop)+")"  + ', '
        string = string + ' )'
        print(string)
        for i in tuples:
            # [[('1', '2'), ('2', '3', '4')], [('1', '3'), ('3', '3', '4')]]
            tuple_temp = ()
            tuple_temp = (i,1)
            i.res.append(tuple_temp)
        return string

    def responsible_inputs(self, tuples):
        output = []
        for i in tuples:
            output = output + i.res
            return output


def predicate(id_index, name, tuple_rows):
    #judge if id == name for this row
    # tuple_row: [Atuple,Atuple,Atuple...]
    pred_out = []
    if(id_index < 0 or name == ''):
        return tuple_rows
    else:
        for row in tuple_rows:
            # row -- Atuple
            if(id_index > len(row.tuple)):
                return None
            else:
                if (row.tuple[id_index] == name):
                    pred_out.append(row)
    return pred_out

#count the avg of the key = id
# input_list
def avg(id, input_list):
    length = len(input_list)
    sum = 0
    for row in input_list:
        sum = sum + int(row.tuple[id])
    if length != 0:
        return sum/length
    else:
        return 0

# true list
def comparator(isasc, input_list,index):
    if isasc == True:
        #sort the last col asc
        output = sorted(input_list,key=(lambda x:x.tuple[index]))
    else:
        #sort the last col des
        output = sorted(input_list,key=(lambda x:x.tuple[index]),reverse=True)
    #only output the second col
    return output

def topkcompare(sec,list_i,i):
    # get 2nd high score
    # 0--> not output
    # 1-->counterfactual
    # 2--> res = 0.5
    if(len(list_i)==1):
        return 0
    sum = 0
    for item in list_i:
        if(item == list_i[i]):
            sum = sum + 0
        else:
            sum = sum + int(item[1].tuple[2])
        
    avg = sum / (len(list_i)-1)
    if avg < sec:
        return 1
    else:
        # still top1 but res < 1
        for k in range(len(list_i)):
            # k:0
            sum1 = 0
            for item in list_i:
                # i=2
                if(item == list_i[k] or item == list_i[i] ):
                    sum1 = sum1 + 0
                else:
                    sum1 = sum1 + int(item[1].tuple[2])
            if(len(list_i)-2 == 0):
                avg = 0
            else:
                avg = sum1/(len(list_i)-2)
            if avg < sec:
                return 2
        return 0

if __name__ == "__main__":
    # TASK 1: Implement 'likeness' prediction query for User A and Movie M
    #
    # SELECT AVG(R.Rating)
    # FROM Friends as F, Ratings as R
    # WHERE F.UID2 = R.UID
    #       AND F.UID1 = 'A'
    #       AND R.MID = 'M'

    # YOUR CODE HERE
    syslist = sys.argv
    # print(syslist)
    friend_filepath = syslist[4]
    movie_filepath = syslist[6]
    batch = 300000
    uid = syslist[8]
    logger.info("Assignment #2")
    read_f = Scan(friend_filepath, batch,None, True,True)
    read_m = Scan(movie_filepath, batch,None, True,True)


    # TASK 1: Implement lineage query for movie recommendation

    # YOUR CODE HERE
    if syslist[2] =="1":
        #F.UID1 = 'A'
        friend_s = Select(read_f, batch, 0, uid, predicate,True,True)
        #WHERE F.UID2 = R.UID
        join = Join(batch, friend_s, read_m, 1, 0,True,True)
        #SELECT R.MID, AVG(R.Rating
        project = Project(batch,join,[3,4],True,True)
        #GROUPBY R.MID
        group = GroupBy(batch, project, 0, 1, avg, True,True)
        #ORDERBY score DESC
        order = OrderBy(batch,1,group, comparator, False,True,True)
        #LIMIT 1
        ktop = TopK(batch,order,1,True)
        output = []
        p = ktop.get_next()
        while(p):
            output = output + p
            p = ktop.get_next()
        for row in output:
            print("movie"+str(row.tuple[0])+" has avg score "+str(row.tuple[1]))
        llist = []
        print("Retrieve the lineage:")
        for i in output:
            llist+=i.lineage()
        print(llist)

    # TASK 2: Implement where-provenance query for 'likeness' prediction

    # YOUR CODE HERE
    if syslist[2] =="2":
        #F.UID1 = 'A'
        mid = syslist[10]
        friend_s = Select(read_f, batch, 0, uid, predicate,True,True)
        #R.MID = 'M'
        movie_s = Select(read_m, batch, 1, mid, predicate,True,True)
        #WHERE F.UID2 = R.UID
        join = Join(batch,friend_s, movie_s, 1, 0,True,True)
        group = GroupBy(batch, join, 3, 4,avg,True,True)
        project = Project(batch, group, [1],True,True)
        j = project.get_next()
        #AVG(R.Rating)
        output = []
        print("Retrieve the lineage:")
        while(j):
            output = output + j
            j = project.get_next()
        print(output[0].lineage())
        print("The average is:")
        print(output[0].tuple[0])
        print("Implement Where-provenance query:")
        print(output[0].where(0))


    # TASK 3: Implement how-provenance query for movie recommendation

    # YOUR CODE HERE
    if syslist[2] =='3':
        #F.UID1 = 'A'
        friend_s = Select(read_f, batch, 0, uid, predicate,True,True)
        #WHERE F.UID2 = R.UID
        join = Join(batch, friend_s, read_m, 1, 0,True,True)
        #SELECT R.MID, AVG(R.Rating
        project = Project(batch,join,[3,4],True,True)
        #GROUPBY R.MID
        group = GroupBy(batch,project, 0, 1, avg,True,True)
        #ORDERBY score DESC
        order = OrderBy(batch,1,group, comparator, False,True,True)
        #LIMIT 1
        ktop = TopK(batch,order,1,True,True)
        output = []
        p = ktop.get_next()
        print("Retrieve the lineage:")
        while(p):
            output = output + p
            p = ktop.get_next()
        print(output[0].lineage())
        for row in output:
            print("movie"+str(row.tuple[0])+" has avg score "+str(row.tuple[1]))
        
        print("Implement Where-provenance query:")
        print(output[0].where(0))

        print("Implement How-provenance query:")
        print(output[0].how())

        llist = []
        print("Retrieve the lineage:")
        for i in output:
            llist+=i.lineage()
        print(llist)


    # TASK 4: Retrieve most responsible tuples for movie recommendation

    # YOUR CODE HERE
    if syslist[2] =='4':
        #F.UID1 = 'A'
        friend_s = Select(read_f, batch, 0, uid, predicate,True,True)
        #WHERE F.UID2 = R.UID
        join = Join(batch, friend_s, read_m, 1, 0,True,True)
        #SELECT R.MID, AVG(R.Rating
        project = Project(batch,join,[3,4],True,True)
        #GROUPBY R.MID
        group = GroupBy(batch,project, 0, 1, avg,True,True)
        #ORDERBY score DESC
        order = OrderBy(batch,1,group, comparator, False,True,True)
        #LIMIT 1
        ktop = TopK(batch,order,1,True,True)
        output = []
        p = ktop.get_next()
        print("Retrieve the lineage:")
        while(p):
            output = output + p
            p = ktop.get_next()
        print(output[0].lineage())
        for row in output:
            print("movie"+str(row.tuple[0])+" has avg score "+str(row.tuple[1]))
        
        print("Implement Where-provenance query:")
        print(output[0].where(0))

        print("Implement How-provenance query:")
        print(output[0].how())

        print("Implement responsible-provenance query:")
        print(output[0].responsible_inputs())
