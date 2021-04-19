import setuptools

setuptools.setup(
    name='datapipe',
    version='0.4.0+imp5',
    include_package_data=True,
    packages=setuptools.find_packages(),
    install_requires=[
        "pandas >=1.1.4",
        "SQLAlchemy >=1.3.20, <1.4",
        "psycopg2_binary >=2.8.6",
        "cloudpickle >=1.6.0",
        "PyYAML >=5.3.1",
        "anyconfig >=0.10.0",
        "fsspec >=0.8.7",
        "Pillow >=7.2.0",
        "tqdm >=4.60.0",
        "xlrd >=2.0.1",
        "openpyxl >=3.0.7",
        "toml >=0.10.2",
        "click >=7.1.2",
        "dash >=1.20.0",
        "e8_dash_app @ git+ssh://git@github.com/epoch8/e8_dash_app.git#egg=e8_dash_app"
    ],
    python_requires='>=3.8'
)
