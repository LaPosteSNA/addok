# Installing Addok

## Dependencies

- Redis
- python3.4

## Install using a virtualenv

1. Install dependencies:

        sudo apt-get install redis-server python3.4 python3.4-dev python-pip python-virtualenv virtualenvwrapper

1. create a virtualenv:

        mkvirtualenv addok --python=/usr/bin/python3.4

1. install python packages:

        pip install addok

## What to do next?
Now you certainly want to [configure Addok](config.md), or directly [import data](import.md).
