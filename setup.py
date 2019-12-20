from setuptools import setup, find_packages

setup(
    name="pg_graphql",
    version="0.1.0",
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
    include_package_data=True,
    entry_points={"console_scripts": ["pg_graphql=pg_graphql.cli:main"]},
    install_requires=["flupy"],
    extras_require={
        "dev": [
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
        ],
        "nvim": ["neovim", "python-language-server"],
    },
)
