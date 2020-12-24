import setuptools

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

setuptools.setup(
    name='c12n_pipe',
    version='0.0.1',
    include_package_data=True,
    packages=setuptools.find_packages(),
    install_requires=install_requires,
    python_requires='>=3.8'
)
