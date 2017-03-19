import time
from datetime import datetime
from datetime import timedelta
from urlparse import urljoin

import pytz
import requests

from exceptions import ResponsysClientError
from utils import convert_to_list_of_dicts
from utils import convert_to_table_structure


class ResponsysClient(object):

    RESPONSYS_AUTH_PATH = '/rest/api/v1.1/auth/token'
    RESPONSYS_RATE_LIMIT_WAITING_PERIOD_IN_SECONDS = 60
    RESPONSYS_INVALID_TOKEN_RESPONSE_DETAIL = 'Not a valid authentication token'
    RESPONSYS_RECORD_PROCESS_LIMIT_QUANTITY = 200

    AUTH_TOKEN_REFRESH_THRESHOLD = timedelta(hours=1)
    DEFAULT_REQUEST_TIMEOUT_IN_SECONDS = 60

    def __init__(self, username=None, password=None, login_url=None):
        self.username = username
        self.password = password
        self.login_url = login_url
        self.issued_url = None
        self.auth_token = None
        self.refresh_timestamp = None

    def get_profile_lists(self):
        method = 'GET'
        path = '/rest/api/v1.1/lists'

        response = self.send_request(method, path)

        self._check_for_valid_response(response)

        return response.json()

    def get_profile_list_extensions(self, profile_list):
        method = 'GET'
        path = '/rest/api/v1.1/lists/{}/listExtensions'.format(profile_list)

        response = self.send_request(method, path)

        self._check_for_valid_response(response)

        return response.json()

    def merge_profile_list_members(self, profile_list, profile_dicts, merge_key):
        member_field_names, member_records = convert_to_table_structure(profile_dicts)

        path = '/rest/api/v1.1/lists/{}/members'.format(profile_list)

        response = self.send_profile_list_merge_request(path, merge_key, member_field_names,
                                                        member_records)

        self._check_for_valid_response(response)

        return response.json()

    def merge_profile_list_extension_members(self, profile_list, list_extension, data_dicts,
                                             merge_key):
        member_field_names, member_records = convert_to_table_structure(data_dicts)

        path = ('/rest/api/v1.1/lists/{}/listExtensions/{}/members'
                .format(profile_list, list_extension))

        response = self.send_extension_table_merge_request(path, merge_key, member_field_names,
                                                           member_records)

        self._check_for_valid_response(response)

        return response.json()

    def merge_supplemental_table_members(self, folder, table, data_dicts):
        member_field_names, member_records = convert_to_table_structure(data_dicts)

        path = ('/rest/api/v1.1/folders/{}/suppData/{}/members'
                .format(folder, table))

        response = self.send_supplemental_table_merge_request(path, member_field_names,
                                                              member_records)

        self._check_for_valid_response(response)

        return response.json()

    def get_profile_list_member(self, profile_list, customer_id):
        method = 'GET'
        path = '/rest/api/v1.1/lists/{}/members/'.format(profile_list)
        query_by_customer_id_params = {
            'qa': 'c',
            'id': str(customer_id),
            'fs': 'all'
        }

        response = self.send_request(method, path, params=query_by_customer_id_params)

        self._check_for_valid_response(response)

        members = self._parse_members(response)

        return members[0]

    def get_extension_table_member(self, profile_list, list_extension, user_id):
        method = 'GET'
        path = ('/rest/api/v1.1/lists/{}/listExtensions/{}/members'
                .format(profile_list, list_extension))
        query_by_customer_id_params = {
            'qa': 'c',
            'id': str(user_id),
            'fs': 'all'
        }

        response = self.send_request(method, path, params=query_by_customer_id_params)

        self._check_for_valid_response(response)

        members = self._parse_members(response)

        return members[0]

    def delete_profile_list_member(self, profile_list, customer_id):
        member = self.get_profile_list_member(profile_list, customer_id)

        method = 'DELETE'
        path = '/rest/api/v1.1/lists/{}/members/{}'.format(profile_list, member['RIID_'])

        response = self.send_request(method, path)

        self._check_for_valid_response(response)

    def delete_list_extension_member(self, profile_list, list_extension, customer_id):
        member = self.get_extension_table_member(profile_list, list_extension, customer_id)

        method = 'DELETE'
        path = ('/rest/api/v1.1/lists/{}/listExtensions/{}/members/{}'
                .format(profile_list, list_extension, member['RIID_']))

        response = self.send_request(method, path)

        self._check_for_valid_response(response)

    def send_profile_list_merge_request(self, path, merge_key, member_field_names, member_records):
        self._check_for_record_limit_quantity(member_records)

        json = {
            "recordData": {
                "fieldNames": member_field_names,
                "records": member_records,
                "mapTemplateName": None
            },
            "mergeRule": {
                "htmlValue": "H",
                "optinValue": "I",
                "textValue": "T",
                "insertOnNoMatch": True,
                "updateOnMatch": "REPLACE_ALL",
                "matchColumnName1": merge_key,
                "matchColumnName2": None,
                "matchOperator": "NONE",
                "optoutValue": "O",
                "rejectRecordIfChannelEmpty": None,
                "defaultPermissionStatus": "OPTIN"
            }
        }

        return self.send_request('POST', path, json=json)

    def send_extension_table_merge_request(self, path, merge_key, member_field_names,
                                           member_records):
        self._check_for_record_limit_quantity(member_records)

        json = {
                "recordData": {
                    "fieldNames": member_field_names,
                    "records": member_records,
                    "mapTemplateName": None
                },
                "insertOnNoMatch": True,
                "updateOnMatch": "REPLACE_ALL",
                "matchColumnName1": merge_key
        }

        return self.send_request('POST', path, json=json)

    def send_supplemental_table_merge_request(self, path, member_field_names, member_records):
        self._check_for_record_limit_quantity(member_records)

        json = {
                "recordData": {
                    "fieldNames": member_field_names,
                    "records": member_records,
                    "mapTemplateName": None
                },
                "insertOnNoMatch": True,
                "updateOnMatch": "REPLACE_ALL"
        }

        return self.send_request('POST', path, json=json)

    def send_request(self, method, path, params=None, json=None):
        self._get_access_items()

        url = urljoin(self.issued_url, path)
        headers = {'Authorization': self.auth_token}

        response = self._send_request(method, url, headers=headers, json=json, params=params)

        # this retries the request if authentication is revoked
        if response.status_code == 401 or self._is_invalid_token_response(response):
            self._login()
            headers = {'Authorization': self.auth_token}

            response = self._send_request(method, url, headers=headers, json=json, params=params,
                                          retry=True)

        return response

    def _send_request(self, method, url, params=None, json=None, headers=None, retry=False):
        try:
            response = requests.request(method, url, params=params, json=json, headers=headers,
                                        timeout=self.DEFAULT_REQUEST_TIMEOUT_IN_SECONDS)
        except requests.exceptions.Timeout:
            raise ResponsysClientError('There was a timeout error sending a request to Responsys.'
                                       'Method: {}, URL: {}'.format(method, url))
        except Exception:
            raise ResponsysClientError('There was an unknown error sending a request to Responsys.'
                                       'Method: {}, URL: {}'.format(method, url))

        if not retry and response.status_code == 429:
            time.sleep(self.RESPONSYS_RATE_LIMIT_WAITING_PERIOD_IN_SECONDS)

            response = self._send_request(method, url, headers=headers, retry=True)

        return response

    def _check_for_record_limit_quantity(self, member_records):
        limit = self.RESPONSYS_RECORD_PROCESS_LIMIT_QUANTITY
        if len(member_records) > limit:
            raise ResponsysClientError('A max of {} members may be created or updated at '
                                       'one time.'.format(limit))

    @staticmethod
    def _check_for_valid_response(response, expected_status_code=200):
        if response.status_code != expected_status_code:
            request = response.request

            raise ResponsysClientError('There was an issue sending a request to Responsys. '
                                       'Request Method: {}. Request Path: {}. Request Body: {}.'
                                       'Response Status Code: {}. Response Text: {}.'
                                       .format(request.method, request.path_url, request.body,
                                               response.status_code, response.text))

    def _is_invalid_token_response(self, response):
        invalid = False
        if response.status_code == 500:
            try:
                detail = response.json().get('detail')
            except AttributeError:
                pass
            else:
                if detail == self.RESPONSYS_INVALID_TOKEN_RESPONSE_DETAIL:
                    invalid = True

        return invalid

    @staticmethod
    def _parse_members(response):
        parsed_response = response.json()

        field_names = parsed_response['recordData']['fieldNames']
        records = parsed_response['recordData']['records']

        members = convert_to_list_of_dicts(field_names, records)

        return members

    def _get_access_items(self):
        if not self.auth_token:
            self._login()
        elif self._time_to_refresh_token():
            self._refresh_token()

    def _time_to_refresh_token(self):
        return (self._now_utc_aware() - self.refresh_timestamp) > self.AUTH_TOKEN_REFRESH_THRESHOLD

    @staticmethod
    def _now_utc_aware():
        return datetime.utcnow().replace(tzinfo=pytz.utc)

    def _login(self):
        method = 'POST'
        path = self.RESPONSYS_AUTH_PATH
        url = urljoin(self.login_url, path)
        params = {'user_name': self.username,
                  'password': self.password,
                  'auth_type': 'password'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        response = self._send_request(method, url, params=params, headers=headers)

        if response.status_code != 200:
            raise ResponsysClientError('There was an issue sending a login request '
                                       'to Responsys. Status Code: {}. Response Text: {}'
                                       .format(response.status_code, response.text))

        self._update_access_items(response)

    def _refresh_token(self):
        method = 'POST'
        path = self.RESPONSYS_AUTH_PATH
        params = {'auth_type': 'token'}
        url = urljoin(self.issued_url, path)
        headers = {'Authorization': self.auth_token}

        response = self._send_request(method, url, params=params, headers=headers)

        if response.status_code != 200:
            self._login()
            return

        self._update_access_items(response)

    def _update_access_items(self, response):
        parsed_response = response.json()

        # this comes back in javascript format and must be converted
        unaware_timestamp = datetime.utcfromtimestamp(parsed_response['issuedAt']/1000)
        utc_timestamp = unaware_timestamp.replace(tzinfo=pytz.UTC)

        self.refresh_timestamp = utc_timestamp
        self.auth_token = parsed_response['authToken']
        self.issued_url = parsed_response['endPoint']
