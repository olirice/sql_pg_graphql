from setuptools import setup, find_packages


DEV_REQUIRES = [
    "black",
    "click",
    "mkdocs",
    "pylint",
    "pygments",
    "psycopg2-binary",
    "pytest",
    "pytest-benchmark",
    "sqlalchemy",
    "sqlparse",
]


setup(
    name="pg_graphql",
    version="0.1.2",
    description="pg_graphql: GraphQL support for PostgreSQL",
    author="Oliver Rice",
    author_email="oliver@oliverrice.com",
    license="TBD",
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: SQL",
    ],
    python_requires=">=3.6",
    packages=find_packages("src/main/python"),
    package_dir={"": "src/main/python"},
    entry_points={"console_scripts": ["pg_graphql=pg_graphql.cli:main"]},
    install_requires=["flupy"],
    extras_require={"dev": DEV_REQUIRES, "nvim": ["neovim", "python-language-server"]},
    include_package_data=True,
)
