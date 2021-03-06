from setuptools import setup, find_packages

with open("README.md") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
        name="pipeliner",
        description="Simple async pipeline orchestrator",
        long_description=readme,
        author="Emil Tylén",
        license=license,
        packages=find_packages(exclude=("tests",)),
        python_requires=">=3.7",
        tests_require=["aiounittest==1.4.0"],
        test_suite="pytest",
)
