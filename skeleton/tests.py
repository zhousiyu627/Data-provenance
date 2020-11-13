import pytest
import csv
import assignment_12
from itertools import islice
import random 

# I use hardcode. Giving the simple input and check is the out same as 
# the predication.

def test_scan():
    file1 = open("data/test1.txt","w")
    file1.write("1 3"+"\n")
    file1.write("2 4")
    file1.close()
    scan = assignment_12.Scan("data/test1.txt", 20, None, True,True)
    # Batch = 1 should output ([1, 3],)
    # If batch = 2 should output ([1, 3],[2,4])
    s = scan.get_next()
    
    assert s[0].tuple == ('1','3')
    assert s[0].lineage()[0] == s[0]
    assert s[0].where(0) == [[('data/test1.txt', 1, s[0], '3')]]
    assert s[0].how() == 'Scan( (t1) )'
    assert s[0].responsible_inputs() == [(s[0],1)]

def test_select():
    file1 = open("data/test1.txt","w")
    file1.write("1 3"+"\n")
    file1.write("2 4")
    file1.close()
    scan = assignment_12.Scan("data/test1.txt", 10, None, True,True)
    # If the first col equals to 2, output
    # So th output is only [2, 4]
    select = assignment_12.Select(scan, 10, 0, '2', assignment_12.predicate,True,True)
    s = select.get_next()
    assert s[0].tuple == ('2','4')
    assert s[0].lineage()[0] == s[0]
    assert s[0].where(0) == [[('data/test1.txt', 2, s[0], '4')]]
    assert s[0].how() == 'Select( (t2) )'
    assert s[0].responsible_inputs() == [(s[0],1)]

def test_project():
    file1 = open("data/test1.txt","w")
    file1.write("1 3"+"\n")
    file1.write("2 4")
    file1.close()
    scan = assignment_12.Scan("data/test1.txt", 10, None, True,True)
    # If the input list is null
    # So the output should be the same as input.
    project = assignment_12.Project(10,scan,[0,1],True,True)
    p = project.get_next()
    assert p[0].tuple == ('1','3')
    assert p[0].lineage()[0] == p[0]
    assert p[0].where(0) == [[('data/test1.txt', 1, p[0], '3')]]
    assert p[0].how() == 'Project( t1 )'

def test_join():
    file1 = open("data/test1.txt","w")
    file1.write("1 3")
    file1.close()
    left = assignment_12.Scan("data/test1.txt", 10, None, True,True)

    file2 = open("data/test2.txt","w")
    file2.write("3 1")
    file2.close()
    right = assignment_12.Scan("data/test2.txt", 10, None, True,True)
    # 1 3 join with 3 1 if 3 == 3
    # so that would be 1 3 3 1
    join = assignment_12.Join(20,left, right, 1, 0,True,True)
    p = join.get_next()
    assert p[0].tuple == ('1', '3', '3', '1')
    assert p[0].lineage()[0][0].tuple == ('1', '3')
    assert str(p[0].where(0)) == "[[('data/test1.txt', 1, ('1', '3'), '3'), ('data/test2.txt', 1, ('3', '1'), '1')]]"
    assert p[0].how() == 'Join( (t1*t1@1) )'

def test_groupby():
    file1 = open("data/test1.txt","w")
    file1.write("4 3"+"\n")
    file1.write("2 2"+"\n")
    file1.write("1 2"+"\n")
    file1.write("3 4"+"\n")
    file1.write("3 3"+"\n")
    file1.write("4 1"+"\n")
    file1.write("3 5")
    file1.close()
    scan = assignment_12.Scan("data/test1.txt",10, None, True,True)
    group = assignment_12.GroupBy(20,scan, 1, 0, assignment_12.avg,True,True)
    #Fisrt col is movie id and second col is rate
    #with the same id, output the avg
    #So for movie id  =  1 2    3      4
    #We have      avg =  2 2 4+3+5/3 1+3/2
    p = group.get_next()
    assert p[0].tuple == ('3', 3.5)
    assert str(p[0].lineage()[0]) == "[('4', '3'), ('3', '3')]"
    assert str(p[0].where(0)) == "[[('data/test1.txt', 1, ('4', '3'), '3'), ('data/test1.txt', 5, ('3', '3'), '3')]]"
    assert p[0].how() == 'GroupBy( t1, t5 )'
    
def test_orderby():
    file1 = open("data/test1.txt","w")
    file1.write("1 80"+"\n")
    file1.write("2 3"+"\n")
    file1.write("3 100")
    file1.close()
    scan = assignment_12.Scan("data/test1.txt",10, None, True,True)
    order = assignment_12.OrderBy(10,1,scan, assignment_12.comparator, True,True,True)
    p = order.get_next()
    assert p[0].tuple == ('3', '100')
    assert str(p[0].lineage()[0]) == "('3', '100')"
    assert str(p[0].where(0)) == "[[('data/test1.txt', 3, ('3', '100'), '100')]]"
    assert p[0].how() == 'OrderBy( t3 )'

def test_limit():
    scan1 = assignment_12.Scan("data/f_t.txt", 20, None, True,True)
    scan2 = assignment_12.Scan("data/m_t.txt", 20, None, True,True)
    friend_s = assignment_12.Select(scan1, 20, 0, "1", assignment_12.predicate,True,True)
    #R.MID = 'M'
    # movie_s = assignment_12.Select(scan2, 20, 1, "3", assignment_12.predicate,True,True)
    #WHERE F.UID2 = R.UID
    join = assignment_12.Join(20,friend_s, scan2, 1, 0,True,True)
    group = assignment_12.GroupBy(20, join, 3, 4,assignment_12.avg,True,True)
    project = assignment_12.Project(20, group,[0,1],True,True)
    order = assignment_12.OrderBy(20,1,project, assignment_12.comparator, False,True,True) 
    #LIMIT 1
    ktop = assignment_12.TopK(20,order,1,True,True)
    p = ktop.get_next()
    assert p[0].tuple == ('2', 3.0)
    assert str(p[0].lineage()[0]) == "[[('1', '2'), ('2', '2', '4')], [('1', '3'), ('3', '2', '3')], [('1', '4'), ('4', '2', '2')]]"
    assert str(p[0].where(0)) == "[[('data/m_t.txt', 5, ('2', '2', '4'), '4'), ('data/m_t.txt', 8, ('3', '2', '3'), '3'), ('data/m_t.txt', 11, ('4', '2', '2'), '2')]]"
    assert p[0].how() == 'TopK( (f1*r5@4), (f2*r8@3), (f3*r11@2) )'
    assert str(p[0].responsible_inputs()) == "[(('1', '2'), 1), (('2', '2', '4'), 1), (('1', '3'), 0.5), (('3', '2', '3'), 0.5)]"


def testall():
    scan1 = assignment_12.Scan("data/f_t.txt", 20, None, True,True)
    scan2 = assignment_12.Scan("data/m_t.txt", 20, None, True,True)
    friend_s = assignment_12.Select(scan1, 20, 0, "1", assignment_12.predicate,True,True)
    #R.MID = 'M'
    # movie_s = assignment_12.Select(scan2, 20, 1, "3", assignment_12.predicate,True,True)
    #WHERE F.UID2 = R.UID
    join = assignment_12.Join(20,friend_s, scan2, 1, 0,True,True)
    group = assignment_12.GroupBy(20, join, 3, 4,assignment_12.avg,True,True)
    project = assignment_12.Project(20, group,[0,1],True,True)
    order = assignment_12.OrderBy(20,1,project, assignment_12.comparator, False,True,True) 
    #LIMIT 1
    ktop = assignment_12.TopK(20,order,1,True,True)
    s = ktop.get_next()
    print(s[0].tuple)
    print(s[0].lineage())
    print(s[0].where(0))
    print(s[0].how())
    print(s[0].responsible_inputs()) 


if __name__ == "__main__":
    # print(test)
    # test_select()
    # test_project()
    # test_scan()
    # test_join()
    # test_avg()
    # test_groupby()
    # test_orderby()
    # test_limit()
    # test_hist()
    testall()