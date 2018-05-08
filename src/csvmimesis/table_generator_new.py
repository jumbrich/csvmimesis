import random
import pandas as pd
import os
import numpy as np




def write_tables(tables, prefix="", path=None):

    if path:
        import pathlib
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)


    for i, t in enumerate(tables):

        df = pd.DataFrame(t)
        rows = df.shape[0]
        cols = df.shape[1]
        if prefix:
            file = "{}-t{}_{}_{}.csv".format(prefix, i + 1, rows, cols)
        else:
            file = "t{}_{}_{}.csv".format(i + 1, rows, cols)
        if path:
            file = os.path.join(path, file)
        print("Printing table{}-{}_{} to {}".format(i+1, rows, cols, file))

        #df = df[header]
        df.to_csv(file,  encoding='utf-8-sig', index=False)


def print_tables(tables):
    for i,t in enumerate(tables):
        s="{:=^80}"
        print(s.format(" Table{} ".format(i+1)))
        df = pd.DataFrame(t)
        #df = df[header]
        print(df)
        #print(df.describe(include = ['O']))
        print("unique values per column")
        print(df.T.apply(lambda x: x.nunique(), axis=1))



def create_data_provider_list(providers, size,  max_tries=100, local=None):
    from mimesis import Generic
    data = {}
    header = []

    for ps in providers:
        if isinstance(ps, tuple):
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
        data[p_name] = create_data_single_provider(p, size, uniquness=u,max_tries=max_tries)
    return header, data

def create_data_single_provider(provider, size, uniquness=1, max_tries=100):
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






#with at least 100 unique values
['address.address', 'address.calling_code', 'address.city', 'address.country', 'address.country_code',
 'address.latitude', 'address.longitude', 'address.postal_code', 'address.street_name', 'address.street_number',
 'address.zip_code',

 'business.company', 'business.copyright', 'business.currency_iso_code', 'business.price', 'business.price_in_btc',

 'code.ean', 'code.imei', 'code.isbn', 'code.issn', 'code.locale_code', 'code.pin',

 'cryptographic.bytes', 'cryptographic.hash', 'cryptographic.mnemonic_phrase', 'cryptographic.salt',
 'cryptographic.token', 'cryptographic.uuid',

 'datetime.date', 'datetime.datetime', 'datetime.time', 'datetime.timestamp', 'datetime.timezone',
 'datetime.week_date',
 'development.version',

 'file.file_name', 'file.mime_type', 'file.size',

 'food.dish', 'food.drink', 'food.fruit', 'food.vegetable',

 'games.game',

 'hardware.phone_model',

 'internet.content_type', 'internet.emoji', 'internet.home_page', 'internet.image_placeholder', 'internet.ip_v4',
 'internet.ip_v6', 'internet.mac_address', 'internet.network_protocol', 'internet.port', 'internet.subreddit',
 'internet.top_level_domain',

 'numbers.between',

 'path.dev_dir', 'path.project_dir', 'path.user', 'path.users_folder',

 'payment.bitcoin_address', 'payment.cid', 'payment.credit_card_expiration_date', 'payment.credit_card_number',
 'payment.cvv', 'payment.ethereum_address', 'payment.paypal',

 'person.avatar', 'person.email', 'person.favorite_movie', 'person.full_name', 'person.identifier', 'person.last_name',
 'person.name', 'person.occupation', 'person.password', 'person.social_media_profile', 'person.surname',
 'person.telephone', 'person.username',

 'science.atomic_number', 'science.chemical_element', 'science.dna', 'science.rna',

 'structure.css', 'structure.css_property', 'structure.html', 'structure.html_attribute_value', 'structure.json',

 'text.hex_color', 'text.rgb_color', 'text.swear_word', 'text.text', 'text.word',

 'transport.airplane', 'transport.truck', 'transport.vehicle_registration_code']


#tempalte
# {
#         'local':'de',
#         'shared_providers':[ ('address.city',1),('address.country',1),('address.postal_code',1)],
#         'add_providers':None,
#         'join_providers':None,
#         'rows':[10,10],
#         'prefix':"address_vert_small"
#     }

t_pairs=[
    {
        'local':'de',
        'shared_providers':[ ('address.city',1),('address.country',1),('address.postal_code',1)],
        'add_providers':None,
        'join_providers':None,
        'rows':[20,20],
        'prefix':"address_vert_no_dups_S"
    },


    {
        'local':'de',
        'shared_providers':[ ('address.city',1),('address.country',1),('address.postal_code',1),('address.latitude',1), ('address.longitude',1)],
        'add_providers':None,
        'join_providers':None,
        'rows':[100,100],
        'prefix':"address_vert_no_dups_L"
    },
    {
        'local':'de',
        'shared_providers':None,
        'add_providers':[ [ ('address.country',1),('address.calling_code',1) ],
                          [ ('address.latitude',1), ('address.longitude',1) ]
                        ],
        'join_providers':[('address.city',1)],
        'rows':[20,20],
        'prefix':"join_no_shared_no_dups_S"
    },
    {
        'local':'de',
        'shared_providers':None,
        'add_providers':[ [ ('person.last_name',1),('person.name',1) ],
                          [ ('person.username',1), ('person.email',1),('address.country',1) ]
                        ],
        'join_providers':[('person.identifier',1)],
        'rows':[20,20],
        'prefix':"person_join_no_shared_no_dups_S"
    }

]
for tp in t_pairs:
    local=tp.get('local',None)
    shared_providers=tp.get('shared_providers',None)
    add_providers=tp.get('add_providers',None)
    join_providers=tp.get('join_providers',None)
    rows=tp['rows']


    _tables = create_table_pair(shared_providers=shared_providers,
                                add_providers=add_providers,
                                join_providers=join_providers,
                                rows=rows, local=local)
    print_tables(_tables)

    dir = os.path.join("/Users/jumbrich/data/mimesis_csvs",tp['prefix'])
    write_tables(_tables, path=dir, prefix=tp['prefix'])


