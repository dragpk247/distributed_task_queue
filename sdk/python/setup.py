#!/usr/bin/env python3
"""Setup script for distributed-task-queue Python SDK."""

from setuptools import find_packages, setup

setup(
    name="distributed-task-queue",
    version="0.1.0",
    description="Pure Python Client and Worker SDK for Distributed Task Queue (RESP)",
    author="Distributed Task Queue Contributors",
    packages=find_packages(),
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
