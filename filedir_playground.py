#%%
import fsspec

#%%
import re

path_template = '/Users/elephantum/Epoch8/Pipe/c12n-pipe/tests/mnist/testSet/testSet/img_{id}.jpg'


def template_to_attrnames(tmpl):
    return re.findall(r'\{([^/]+?)\}', tmpl)

def template_to_glob(tmpl):
    return re.sub(r'\{([^/]+?)\}', '*', tmpl)

def template_to_match(tmpl):
    return re.sub(r'\{([^/]+?)\}', r'(?P<\1>[^/]+?)', tmpl)

#%%
test_filename = '/Users/elephantum/Epoch8/Pipe/c12n-pipe/tests/mnist/testSet/testSet/img_1.jpg'

#%%
m = re.match(
    template_to_match(path_template),
    test_filename
)

if m is not None:
    print(m.group('id'))

# %%
ff = fsspec.open_files()
# %%
len(ff)
# %%
ff[0]
# %%
f = ff[0]
# %%
f
# %%
f.fs.info(f.path)
# %%
ff.fs
# %%
