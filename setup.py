import setuptools

with open('requirements.txt') as f:
    install_requires = f.read().splitlines()

setuptools.setup(
    name='c12n_pipe',
    version='0.1.2',
    include_package_data=True,
    packages=setuptools.find_packages(),
    install_requires=install_requires,
    python_requires='>=3.8',
    entry_points={
        'console_scripts': [
            'label-studio-c12n=c12n_pipe.label_studio_utils.label_studio_c12n:main',
        ],
    }
)
