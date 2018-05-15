import random
import pandas as pd
import os
import numpy as np
import logging
log = logging.getLogger(__name__)

import pathlib

def write_table(table, dir=None, file=None, prefix=None):

    df = pd.DataFrame(table)
    rows = df.shape[0]
    cols = df.shape[1]

    if file:
        pathlib.Path(os.path.dirname(file)).mkdir(parents=True, exist_ok=True)
    else:
        if prefix:
            file="{}_{}_{}.csv".format(prefix, rows, cols)
        else:
            file = "table_{}_{}.csv".format(rows, cols)
        if dir:
            file = os.path.join(dir, file)

    print(os.path.dirname(file))
    pathlib.Path(os.path.dirname(file)).mkdir(parents=True, exist_ok=True)
    log.info("Writing table to %s",file)
    df.to_csv(file,  encoding='utf-8', index=False)


def write_tables(tables, prefix="", dir=None):
    for i, t in enumerate(tables):
        if prefix:
            prefix =  "{}-t{}".format(prefix, i+1)
        else:
            prefix = "t{}".format(i+1)
        write_table(t, prefix=prefix, dir=dir, file =None)


def print_table(table, prefix=""):
    s = "{:=^80}"
    print(s.format(" Table{} ".format(prefix)))
    df = pd.DataFrame(table)
    # df = df[header]
    print(df)
    # print(df.describe(include = ['O']))
    print("unique values per column")
    print(df.T.apply(lambda x: x.nunique(), axis=1))


def print_tables(tables):
    for i,t in enumerate(tables):
        print_table(t,prefix=i + 1)



def create_data_provider_list(providers, size,  max_tries=100, local=None):
    from mimesis import Generic
    data = {}
    header = []

    for ps in providers:
        if isinstance(ps, tuple):
            p = ps[0]
            u = ps[1]
        elif isinstance(ps, list):
            p = ps[0]
            u = ps[1]
        else:
            p = ps
            u = None

        if isinstance(p,str):
            generic = Generic(local)
            s=p.split('.')
            p=getattr(getattr(generic, s[0]),s[1])
        p_name = p.__name__
        header.append(p_name)
        data[p_name] = create_data_single_provider(p, size, uniquness=u, max_tries=max_tries)
    return header, data

def create_data_single_provider(provider, size, uniquness=1, max_tries=100):
    log.debug("Create %s items from %s", size, provider)
    if uniquness is None:
        return [provider() for i in range(0,size)]

    uniq=set([])
    u_values= max(int(size*uniquness),1)

    data=[]
    while len(uniq) < u_values:
        c=0
        while True:
            value = provider()
            c+=1
            if value not in uniq:
                break
            if c > max_tries:
                raise Exception("Could not create another unqiue value")
        uniq.add(value)
        data.append(value)

    while len(data)<size:
        value = random.choice(list(uniq))
        data.append(value)
    random.shuffle(data)
    return data


def create_table_pair( shared_providers=None, add_providers=None, join_providers=None,rows=None, local=None):
    if isinstance(rows, list) and len(rows)==2:
        t1_rows=rows[0]
        t2_rows=rows[1]
    elif isinstance(rows, int):
        t1_rows = rows
        t2_rows = rows
    else:
        raise Exception("Cannot parse rows paramter {}".format(rows))

    _tables = [{},{}]
    if shared_providers:
        header, data= create_data_provider_list(shared_providers, t1_rows+t2_rows, local=local)
        _tables[0].update({h: data[h][0:t1_rows] for h in header})
        _tables[1].update({h: data[h][t1_rows:t1_rows+t2_rows] for h in header})
    if add_providers:
        t1_providers= add_providers[0]
        t2_providers = add_providers[1]

        header, data = create_data_provider_list(t1_providers, t1_rows, local=local)
        _tables[0].update({h: data[h] for h in header})

        header, data = create_data_provider_list(t2_providers, t2_rows, local=local)
        _tables[1].update({h: data[h] for h in header})

    if join_providers:
        _rows= max(t1_rows,t2_rows)
        header, data = create_data_provider_list(join_providers, _rows)
        _tables[0].update({h: data[h][0:t1_rows] for h in header})
        _tables[1].update({h: data[h][0:t2_rows] for h in header})


    return _tables





def create_table(tab_profile):
    log.info("Create table from %s", tab_profile)
    local = tab_profile.get('local',None)
    rows = tab_profile.get('rows',None)
    columns = tab_profile.get('columns',None)

    providers=[]
    for c_prov in columns:
        if isinstance(c_prov, list):
            providers.append((c_prov[0],c_prov[1]))
        else:
            providers.append(c_prov)
    header, data = create_data_provider_list(providers,rows,local=local)
    table={h: data[h] for h in header}


    return table




#with at least 100 unique values
