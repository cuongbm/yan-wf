import setuptools

with open("README.md", "r") as fh:

    long_description = fh.read()

setuptools.setup(

    name="yanwf",  # Replace with your username

    version="0.0.1",

    author="cuongbui",

    author_email="buiminhcuong@hotmail.com",

    description="Yan workflow",

    long_description=long_description,

    long_description_content_type="text/markdown",

    url="<https://github.com/cuongbui/yan-wf",

    packages=setuptools.find_packages(where="src", exclude=("tests",)),

    package_dir={"": "src"},

    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",

    ],

    python_requires='>=3.6',
)
