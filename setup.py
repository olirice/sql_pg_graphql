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

ENTRY_POINTS = {
    "console_scripts": ["pg_graphql=pg_graphql.cli:main"],
    "pygments.lexers": ["graphqllexer=pg_graphql.lexer:GraphQLLexer"],
}


setup(
    name="pg_graphql",
    version="0.1.3",
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
    entry_points=ENTRY_POINTS,
    install_requires=["flupy", "graphql-core>=3a"],
    extras_require={"dev": DEV_REQUIRES, "nvim": ["neovim", "python-language-server"]},
    include_package_data=True,
)
