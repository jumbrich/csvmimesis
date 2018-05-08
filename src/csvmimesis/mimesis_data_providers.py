import inspect
from mimesis import Generic

def show_mimesis():
    generic = Generic()
    for c in dir(generic):
        for cc in inspect.getmembers(getattr(generic,c), predicate=inspect.ismethod):
            if not cc[0].startswith("_"):
                print("{}.{}".format(c,cc[0]))

def print_mimesis(provider=None):
    generic = Generic()
    for c in dir(generic):
        if not provider or provider == c:
            print("{:=^80}".format(c.center(len(c)+2)))
            for cc in inspect.getmembers(getattr(generic,c), predicate=inspect.ismethod):
                if not cc[0].startswith("_"):
                    print(" * {}".format(cc[0]))


def print_unique(provider=None, method=None, local=None, max=1000):
    generic = Generic(local)
    data = create_data(max_unique=max, local=local, provider=provider, method=method)
    for k, v in data.items():
        print("{:=^80}".format(k.center(len(k)+2)))
        for kk, vv in v.items():
            print(" * {:>20} {}".format(kk, vv))

def create_data(local=None, provider=None, method=None, max_unique=100):
    generic = Generic(local)
    data={}
    for c in dir(generic):
        data[c]={}
        for cc in inspect.getmembers(getattr(generic,c), predicate=inspect.ismethod):
            if not cc[0].startswith("_"):
                d=set([])
                max_tries=100
                a = 0
                try:
                    while len(d)<max_unique:
                        value = cc[1]()
                        if value not in d:
                            d.add(value)
                            a=0
                        else:
                            a += 1
                        if a > max_tries:
                            break

                except Exception:
                    pass
                data[c][cc[0]]=len(d)
    return data

def print_providers(data=None, local=None, max_unqiue=10000, only_max=False):

    max_unique = max_unqiue
    data = data if data else create_data(max_unique=max_unique, local=local)
    for k, v in data.items():
        for kk, vv in v.items():
            if not only_max or vv == max_unique:
                print("{:>15}.{:<15} {}".format(k, kk, vv))

def list_providers(data=None, local=None, max_unqiue=10000, only_max=False):
    max_unique = max_unqiue
    d=[]
    data = data if data else create_data(max_unique=max_unique, local=local)
    for k, v in data.items():
        for kk, vv in v.items():
            if not only_max or vv == max_unique:
                d.append( (k,kk))
    return d


