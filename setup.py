from setuptools import setup, find_packages

setup(
    name="car_crash_solution",
    version="1.0.0",
    author="kinjal",
    author_email="kinjal4.singh@gmail.com",
    description="BCG Case Study Solution ",
    license="-",
    url="",
    packages=find_packages() + ["config"],
    data_files=[
        (
            "config",
            ["config/config.json"]
        )
    ]
)
