
# coding: utf-8

# ## Program: searcher_journey.ipynb
# 
# Purpose: to map the journey of searchers
# 
# Date of Creation: 9/10/2018
# 
# Date of Modification: 1/8/2019
# 
# Analyst: Aaron Lai
# 
# Remark:

# In[1]:


# import packages
import numpy as np
import pandas as pd
import ngram

import psycopg2 as ps
import re

import yaml
import sqlalchemy as sa

import time

import matplotlib


# In[39]:


# set up credential for gp3 connection
credentials = yaml.load(open('/Users/alai/Documents/credentials.yml'))

host_name = credentials['pg_host']
dbname = credentials['pg_dbname']
user_name = credentials['pg_user']
pswd = credentials['pg_password']
port_no = 5432


# In[40]:


# set up sqlalchemy connection

def get_sqlalchemy_url():
    """ db url template maker for sqlalchemy postrgresql """
    template = '{dialect}+{driver}://{user}:{pass}@{host}:{port}/{database}'
    db_url_params = {
        'dialect': 'postgresql',
        'driver': 'psycopg2',
        'host': host_name,
        'database': dbname,
        'user': user_name,
        'port': port_no,
        'pass': pswd
    }
    return template.format(**db_url_params)

def get_sqlalchemy_engine():
    """ get the engine instance """
    db_url = get_sqlalchemy_url()
    return sa.create_engine(db_url)


# In[41]:


# connecting to the database
conn = ps.connect(host=host_name,
database=dbname,
user=user_name,
password=pswd,
port=port_no)

cursor = conn.cursor()


# In[43]:


# testing connection
cursor = conn.cursor()

sql_code = "select count(*) as n from strata_scratch.ae_ef_lookup_tbl;"
cursor.execute(sql_code)

data_fetch = cursor.fetchall()
print(data_fetch)

df = pd.read_sql(sql_code, conn)
df


# In[7]:


# create a 100 accounts random sample -- have event
sql_random_acct = '''
CREATE TABLE provider_listing.tmp_random_acct as 
select DISTINCT account_key
from edw.fact_event
where
  event_source not in ('soa')
  and lower(event_name) not in ('uri','account_id', 'init', 'tabspage', 'main', 'homepage')
  and lower(event_name) not like '%login%'
  and lower(event_name) not like '%password%'
  and substr(event_name,1,2) not in ('t0','t1','t2')
  and account_key is not null
  and event_date_key between 20180101 and 20180930
order BY random()
LIMIT 500
'''
# create a 100 accounts random sample -- have search
sql_random_acct = '''
CREATE TABLE provider_listing.tmp_random_acct as
select DISTINCT account_key
from edw.fact_search s
where
event_source='ikkuna' 
and s.event_name='search'
and s.event_search_type='procedure'
and provider_id <> '0_UKN'
and s.search_date_key between 20180101 and 20180930
order BY random()
LIMIT 500;
'''


# In[8]:


cursor.execute('drop table IF EXISTS provider_listing.tmp_random_acct')


# In[9]:


# run the create table statement
cursor.execute(sql_random_acct)


# In[44]:


# create event table based on the sample
sql_event_only = '''
create TABLE provider_listing.tmp_event_only as
select distinct
  e.account_key,
  e.event_timestamp,
  e.event_name,
  e.event_source,
  e.event_id,
  CASE
    WHEN lower(e.event_name) like '%tracker%' and lower(e.event_name) like '%%' then 't'
    WHEN lower(e.event_name) like '%choosing%' and lower(e.event_name) like '%wisely%' then 'w'
    WHEN lower(e.event_name) like '%find%' and lower(e.event_name) like '%%' then 'f'
    WHEN lower(e.event_name) like '%your_plan%' and lower(e.event_name) like '%%' then 'l'
    WHEN lower(e.event_name) like '%insurancecard%' and lower(e.event_name) like '%%' then 'i'
    WHEN lower(e.event_name) like '%vision%' and lower(e.event_name) like '%%' then 'v'
    WHEN lower(e.event_name) like '%education%' and lower(e.event_name) like '%%' then 'e'
    WHEN lower(e.event_name) like '%provider%' and lower(e.event_name) like '%%' then 'p'
    WHEN lower(e.event_name) like '%academy%' and lower(e.event_name) like '%%' then 'A'
    WHEN lower(e.event_name) like '%spotlight%' and lower(e.event_name) like '%%' then 'S'
    WHEN lower(e.event_name) like '%search%' and lower(e.event_name) like '%%' then 's'
    WHEN lower(e.event_name) like '%benefit%' and lower(e.event_name) like '%%' then 'b'
    WHEN lower(e.event_name) like '%notification%' and lower(e.event_name) like '%%' then 'n'
    WHEN lower(e.event_name) like '%claim%' and lower(e.event_name) like '%%' then 'c'
    WHEN lower(e.event_name) like '%claim%' and lower(e.event_name) like '%recent%' then 'C'
    WHEN lower(e.event_name) like '%provider%' and lower(e.event_name) like '%quality%' then 'P'
    WHEN lower(e.event_name) like '%find%' and lower(e.event_name) like '%care%' then 'F'
    else '.' end as event_code
from edw.fact_event e
where
  event_source not in ('soa')
  and lower(event_name) not in ('uri','account_id', 'init', 'tabspage', 'main', 'homepage')
  and lower(event_name) not like '%login%'
  and lower(event_name) not like '%password%'
  and substr(event_name,1,2) not in ('t0','t1','t2')
  and account_key is not null
  and event_date_key between 20180101 and 20180930
  and account_key in (select * from provider_listing.tmp_random_acct)
distributed by (account_key, event_timestamp)
'''


# In[45]:


cursor.execute('drop table IF EXISTS provider_listing.tmp_event_only')


# In[46]:


# run the create table statement
cursor.execute(sql_event_only)
cursor.execute('commit')


# In[47]:


# insert the search information
sql_search = '''
insert into provider_listing.tmp_event_only
SELECT distinct
  s.account_key,
  s.src_search_event_timestamp,
  s.event_name ,
  s.event_source,
  s.src_event_id,
  case when event_source='ikkuna' and provider_id <> '0_UKN' and s.event_search_type='procedure' then 'E'
    WHEN event_source='ventana' and provider_id <> '0_UKN' and s.event_search_type='procedure' then 'R'
    ELSE 's' end as edp_srp
from edw.fact_search s
where
s.event_name='search'
and s.search_date_key between 20180101 and 20180930
and account_key in (select * from provider_listing.tmp_random_acct)
'''


# In[48]:


# run the create table statement
cursor.execute(sql_search)
cursor.execute('commit')


# In[52]:


# check number of records
cursor = conn.cursor()

sql_code = "select count(*) as n from provider_listing.tmp_event_only;"
cursor.execute(sql_code)

data_fetch = cursor.fetchall()
print(data_fetch)


# In[53]:


# read the event table in
sql_code = "select * from provider_listing.tmp_event_only;"
cursor.execute(sql_code)

df = pd.read_sql(sql_code, conn)


# In[54]:


df.head()


# In[55]:


# save the table to a local file
store = pd.HDFStore('/Users/alai/Documents/search/event.h5')

store['seq'] = df  # save it


# In[11]:


# load the table to a local file
store = pd.HDFStore('/Users/alai/Documents/search/event.h5')

seq = store['seq']  # load it
seq.head()


# In[12]:


# check the number of records
len(seq)


# In[13]:


# sort the data by account and time (using event_id as proxy)
seq.sort_values(by=['account_key', 'event_id'], inplace=True)

seq.head()


# In[14]:


# transform the coded events into a string by user
# this is using dict

seqlist = {}
j = 0
eventlist = ''
acct = ''
lacct = ''
lchar = ''

for i,ele in enumerate(seq.values):
    # assign the first one
    if i == 0:
        acct = seq.iloc[1,0]
        lacct = seq.iloc[1,0]
        lchar = seq.iloc[1,5]
        eventlist = str(seq.iloc[i,5])
    # capture user ID
    acct = seq.iloc[i,0]
    
    # check if it is a new user
    if acct != lacct:
        # assign to dict
        seqlist[lacct] = eventlist
        # cleanup value
        eventlist = ''
        lacct = acct       
    else:
        if seq.iloc[i,5] != lchar:
            eventlist = eventlist + str(seq.iloc[i,5])
    
    lchar = seq.iloc[i,5]

# fix the last entry issue
seqlist[acct] = eventlist


# In[15]:


# check the first 10 records
outcheck = {k: seqlist[k] for k in list(seqlist)[:10]}
outcheck


# In[16]:


# convert it to a dataframe
seqevent = pd.DataFrame(list(seqlist.items()), columns=['acct','event'])
print('before removal: number of records for seqevent = {0}'.format(len(seqevent)))
# remove those with no event
seqevent = seqevent[seqevent.event != '']
# remove those with single event
seqevent = seqevent[seqevent.event != '.']
# remove those with less than five events
seqevent = seqevent[seqevent.event.str.len() >= 5]
print('after removal: number of records for seqevent = {0}'.format(len(seqevent)))
seqevent.head()


# In[9]:


# pairwise comparison to calculate the ngrams score
# note that this is a dumb approach as it requires n(n-1)/2 computation or O(N^2)

# check for processing time
start_ts = time.time()

pairresult = pd.DataFrame(columns=['firstid','secondid','ngscore'])
for i, ele in enumerate(seqevent.values):
    for j, ele2 in enumerate(seqevent.values):
        if i > j:
            ngscore = ngram.NGram.compare(seqevent.iloc[i,1], seqevent.iloc[j,1])
            firstid = min(seqevent.iloc[i,0], seqevent.iloc[j,0])
            secondid = max(seqevent.iloc[i,0], seqevent.iloc[j,0])
            id = str(firstid) + ',' + str(secondid)
            newrow = [int(firstid), int(secondid), ngscore]
            pairresult.loc[-1] = newrow
            pairresult.index = pairresult.index + 1
            pairresult = pairresult.sort_index()
            
runtime = time.time() - start_ts
print('The pairwise comparison ran for {}'.format(runtime))


# In[10]:


# save the table to a local file
store = pd.HDFStore('/Users/alai/Documents/search/cluster.h5')

store['seq'] = pairresult  # save it


# In[2]:


# load the table to a local file
store = pd.HDFStore('/Users/alai/Documents/search/cluster.h5')

pairresult = store['seq']  # load it
pairresult.head()


# In[9]:


# output it to csv for tableau
print('number of records = {}'.format(len(pairresult)))
pairresult.to_csv('/Users/alai/Documents/search/cluster.csv')


# In[11]:


# ngscore distribution

# check for processing time
start_ts = time.time()

pairresult['ngscore'].plot.hist()

runtime = time.time() - start_ts
print('The histogram ran for {}'.format(runtime))


# In[12]:


# make it print out
pairresult['ngscore'].plot.hist()


# In[23]:


# assigning accounts into cluster

cluster = {} # dict to contain cluster ID and account ID

outbucket = [] # list of account ID that should be excluded or taken

friendlist = {} # dict to contain all friends of an account


cutoff    = 0.03 # cutoff score for similarity score
edge_accp = 0.5  # acceptance level for empirical/thoertical edges count
hardedge  = 0.2  # the hard cut-off for preliminary node removal

# create a df with only edges > cutoff similarity score
df = pairresult[pairresult['ngscore'] >= cutoff]

print('dataframe after cutoff = {0} and {1} records'.format(cutoff, len(df)))
print(df.head())
print('-------')

def find_friend(acct):
    # create a list with all related ID

    # list where acct is first ID
    friend = list(df[df.firstid == acct].secondid)
    friend.append(acct)

    # list where acct is second ID
    tmp    = list(df[df.secondid == acct].firstid)
    tmppair = df[df.secondid == acct][['secondid','firstid']]
    
    # add the tmp list to friend list
    for a in tmp:
        # append if not in yet
        if (a not in friend):
            friend.append(a)
            
    return friend

# loop through account
# pick one here for testing
for i in range(len(seqevent)):
    
    acct = seqevent.iloc[i]['acct']
    
    # stop if an account has already been assigned to a cluster
    if acct in outbucket:
        break 

    # find out all friends
    friends = find_friend(acct)
    
    # exclude those friends who are taken
    friends = list(set(friends) - set(outbucket))
    
    print('acct = {0}; it has {1} friends'.format(acct, len(friends)))
    
    # add this to the friendlist
    friendlist[acct] = friends
    
    # setup the overlap dict
    overlapdict = {}
    
    # first pass: loop each friend to count # of overlap friends
    for f, friend_acct in enumerate(friends):
        
        # check if friends have already been found earlier
        if friend_acct in friendlist.keys():
            friends2 = friendlist[friend_acct]
        else:
            friends2 = find_friend(friend_acct)
            # add results back to friendlist
            friendlist[friend_acct] = friends2
        
        # exclude those friends who are taken
        friends2 =   list(set(friends2) - set(outbucket))
        
        overlap = list(set(friends).intersection(friends2))
        overlapdict[friend_acct] = len(overlap)
        
        # check if the completeness is too far off
        if len(overlap)/len(friends) < hardedge:
            friends.remove(friend_acct)
        else:
            # add to the overlap dict as it is a keep
            overlapdict[friend_acct] = len(overlap)
    

    # sort the overlap dict in ascending order by # of overlap
    sortedoverlap = sorted(overlapdict.items(), key=lambda x: x[1])
    print('len of overlap = {0}'.format(len(sortedoverlap)))
    
    # one last loop to go through each account in forming cluster
    for f in range(len(sortedoverlap)):
        friend_acct = sortedoverlap[f][0]
        friends2 = friendlist[friend_acct]
        overlap = list(set(friends).intersection(friends2))
        
        # check if it is good enough to complete a graph
        if len(overlap)/len(friends) >= edge_accp:
            # condition met and no longer need to go further
            # add them all into one cluster
            cluster[i] = friends
            outbucket = outbucket + friends
            
            print('for cluster {0}, outbucket has {1} nodes'.format(i, len(outbucket)))
            break
        else:
#             print('f = {1} and friend_acct is {0}'.format(friend_acct, f))
#             print('friends are: {0}'.format(friends))
            if friend_acct in friends:
                friends.remove(friend_acct)

# after looping through all entries, put leftover into one cluster
leftover = set(seqlist)

print('number of clusters = {0}'.format(len(cluster)))
for c in range(len(cluster)):
    print('cluster {0} has {1} nodes'.format(c, len(cluster[c])))
    leftover = leftover - set(cluster[c])

# catch all
cluster[c+1] = list(leftover)
print('cluster {0} has {1} nodes'.format(c+1, len(cluster[c+1])))


# In[66]:


# output the clustering results to csv for tableau

dctdf = pd.DataFrame(columns=['cluster','acct'])

for d in cluster:
    for v in cluster[d]:
        dtmp = pd.DataFrame(data=[[d,v]],columns=['cluster','acct'])
        dctdf = dctdf.append(dtmp)

dctdf.head()   
print('number of obs for cluster = {}'.format(len(dctdf)))

resdf = dctdf.merge(seqevent, on = 'acct')

print(resdf.head())

print('number of obs for merged cluster = {}'.format(len(resdf)))

# export
resdf.to_csv('/Users/alai/Documents/search/rescluster.csv')


# In[37]:


for e, ele in enumerate(cluster[0]):
    print('acct = {0} and seq = {1}'.format(ele, str(seqevent[seqevent.acct == ele].event)))
    if e > 30:
        break


# In[16]:


for e, ele in enumerate(cluster[1]):
    print('acct = {0} and seq = {1}'.format(ele, str(seqevent[seqevent.acct == ele].event)))
    if e > 30:
        break


# In[38]:


for e, ele in enumerate(cluster[2]):
    print('acct = {0} and seq = {1}'.format(ele, str(seqevent[seqevent.acct == ele].event)))
    if e > 30:
        break

