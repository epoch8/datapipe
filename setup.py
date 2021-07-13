import setuptools

setuptools.setup(
    name='datapipe',
    version='0.7.1',
    include_package_data=True,
    packages=setuptools.find_packages(),
    install_requires=[
        "fsspec ==2021.5.0",
        "gcsfs ==2021.5.0",

        "pandas >=1.1.4",
        "SQLAlchemy >=1.3.20, <1.4",
        "psycopg2_binary >=2.8.4",
        "cloudpickle >=1.6.0",
        "PyYAML >=5.3.1",
        "Pillow >=7.2.0, <8",
        "tqdm >=4.60.0",
        "toml >=0.10.2",
        "click >=7.1.2",
        "requests >= 2.24.0",
    ],
    extras_require={
        "excel": [
            "xlrd >=2.0.1",
            "openpyxl >=3.0.7",
        ],
        "ui": [
            "dash >=1.20.0",
            "dash_bootstrap_components >= 0.12.0",
            "dash_interactive_graphviz >=0.3.0",
        ],
        "label-studio": [
            "label-studio >=1.1.0",
        ]
    },
    python_requires='>=3.8'
)
