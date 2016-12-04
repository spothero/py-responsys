# py-responsys

## Overview
This repository houses an api client, ResponsysClient, for use with the Oracle Responsys v1.1 REST
API. Documentation for REST API itself can be found here:
http://docs.oracle.com/cloud/latest/marketingcs_gs/OMCEB/OMCEB.pdf.

This implementation is written in Python 2.7.

The ResponsysClient takes care of authentication and throttling on all public methods. In
addition, the methods dealing with retrieving and merging data deal with dictionaries and lists of
dictionaries in order to simplify the data transformation involved with those methods.

## Getting Started
To get started, simply import the ResponsysClient and initialize an instance with your username,
password and login url. Once this is complete, you may begin using the public methods to interact
with the profile lists, extension tables and supplemental tables. An example of this is shown
below:

```python
from client import ResponsysClient
client = ResponsysClient(username='your_username', password='your_password', login_url='your_login_url')
client.get_profile_lists()
```


