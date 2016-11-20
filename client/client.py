import logging
import time
from datetime import datetime
from datetime import timedelta
from urlparse import urljoin

import pytz
import requests

from .exceptions import ResponsysAPIError
# from third_party.responsys.utils import convert_to_list_of_dicts
# from third_party.responsys.utils import convert_to_table_structure

logger = logging.getLogger(__name__)


class ResponsysAPI(object):

    AUTH_TOKEN_REFRESH_THRESHOLD = timedelta(hours=1)
    AUTH_PATH = '/rest/api/v1.1/auth/token'
    RESPONSYS_RATE_LIMIT_WAITING_PERIOD_IN_SECONDS = 60
    RESPONSYS_INVALID_TOKEN_RESPONSE_DETAIL = 'Not a valid authentication token'
    DEFAULT_REQUEST_TIMEOUT_IN_SECONDS = 60
    RECORD_PROCESS_LIMIT_QUANTITY = 200

    def __init__(self, username=None, password=None, login_url=None, profile_list=None,
                 supplemental_folder_name=None):

        self.username = username
        self.password = password
        self.login_url = login_url
        self.issued_url = None
        self.auth_token = None
        self.refresh_timestamp = None
        self.profile_list = profile_list
        self.supplemental_folder_name = supplemental_folder_name

    def get_profile_contact_lists(self):
        method = 'GET'
        path = '/rest/api/v1.1/lists'

        response = self.send_request(method, path)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        return response.json()

    def get_extension_tables_lists(self):
        method = 'GET'
        path = '/rest/api/v1.1/lists/{}/listExtensions'.format(self.profile_list)

        response = self.send_request(method, path)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        return response.json()

    def create_or_update_contact_list_members(self, contact_dicts):
        member_field_names, member_records = convert_to_table_structure(contact_dicts)

        method = 'POST'
        path = '/rest/api/v1.1/lists/{}/members'.format(self.profile_list)

        response = self.send_merge_request(method, path, member_field_names, member_records)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        return response.json()

    def create_or_update_extension_table_members(self, table_name, data_dicts):
        member_field_names, member_records = convert_to_table_structure(data_dicts)

        method = 'POST'
        path = ('/rest/api/v1.1/lists/{}/listExtensions/{}/members'
                .format(self.profile_list, table_name))

        response = self.send_ext_table_merge_request(method, path, member_field_names,
                                                     member_records)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        return response.json()

    def create_or_update_supplemental_table_members(self, table_name, data_dicts):
        member_field_names, member_records = convert_to_table_structure(data_dicts)

        method = 'POST'
        path = ('/rest/api/v1.1/folders/{}/suppData/{}/members'
                .format(self.supplemental_folder_name, table_name))

        response = self.send_merge_request(method, path, member_field_names, member_records)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        return response.json()

    def get_contact_list_table_member(self, user_id):
        method = 'GET'
        path = '/rest/api/v1.1/lists/{}/members/'.format(self.profile_list)
        query_by_customer_id_params = {
            'qa': 'c',
            'id': str(user_id),
            'fs': 'all'
        }

        response = self.send_request(method, path, params=query_by_customer_id_params)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        members = self._parse_members(response)

        return members[0]

    def get_extension_table_member(self, table_name, user_id):
        method = 'GET'
        path = ('/rest/api/v1.1/lists/{}/listExtensions/{}/members'
                .format(self.profile_list, table_name))
        query_by_customer_id_params = {
            'qa': 'c',
            'id': str(user_id),
            'fs': 'all'
        }

        response = self.send_request(method, path, params=query_by_customer_id_params)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        members = self._parse_members(response)

        return members[0]

    def get_supplemental_table_member(self, table_name, user_id):
        method = 'GET'
        path = ('/rest/api/v1.1/folders/{}/suppData/{}/members'
                .format(self.supplemental_folder_name, table_name))
        query_by_customer_id_params = {
            'qa': 'CUSTOMER_ID_',
            'id': str(user_id),
            'fs': 'all'
        }

        response = self.send_request(method, path, params=query_by_customer_id_params)

        self._check_for_valid_response(method, path, response.status_code, response.text)

        members = self._parse_members(response)

        return members[0]

    def delete_contact_list_member(self, user_id):
        member = self.get_contact_list_table_member(user_id)

        method = 'DELETE'
        path = '/rest/api/v1.1/lists/{}/members/{}'.format(self.profile_list, member['RIID_'])

        response = self.send_request(method, path)

        self._check_for_valid_response(method, path, response.status_code, response.text)

    def delete_extension_table_member(self, table_name, user_id):
        member = self.get_extension_table_member(table_name, user_id)

        method = 'DELETE'
        path = ('/rest/api/v1.1/lists/{}/listExtensions/{}/members/{}'
                .format(self.profile_list, table_name, member['RIID_']))

        response = self.send_request(method, path)

        self._check_for_valid_response(method, path, response.status_code, response.text)

    def delete_supplemental_table_member(self, table_name, user_id):
        method = 'DELETE'
        path = ('/rest/api/v1.1/lists/folders/{}/suppData/{}/members'
                .format(self.supplemental_folder_name, table_name))

        delete_by_customer_id_params = {
            'qa': 'CUSTOMER_ID_',
            'id': str(user_id)
        }

        response = self.send_request(method, path, params=delete_by_customer_id_params)

        self._check_for_valid_response(method, path, response.status_code, response.text)

    @staticmethod
    def _parse_members(response):
        parsed_response = response.json()

        field_names = parsed_response['recordData']['fieldNames']
        records = parsed_response['recordData']['records']

        members = convert_to_list_of_dicts(field_names, records)

        return members

    def send_request(self, method, path, params=None, json=None):
        self._get_access_items()

        url = urljoin(self.issued_url, path)
        headers = {'Authorization': self.auth_token}

        response = self._send_request(method, url, headers=headers, json=json, params=params)

        # this retries the request if authentication is revoked
        if response.status_code == 401 or self._is_invalid_token_response(response):
            self._login()
            headers = {'Authorization': self.auth_token}

            response = self._send_request(method, url, headers=headers, json=json, params=params)

        return response

    def _send_request(self, method, url, params=None, json=None, headers=None, retry=False):
        try:
            response = requests.request(method, url, params=params, json=json, headers=headers,
                                        timeout=self.DEFAULT_REQUEST_TIMEOUT_IN_SECONDS)
        except requests.exceptions.Timeout:
            raise ResponsysAPIError('There was a timeout error sending a request to Responsys.'
                                    'Method: {}, URL: {}'.format(method, url))
        except Exception:
            raise ResponsysAPIError('There was an unknown error sending a request to Responsys.'
                                    'Method: {}, URL: {}'.format(method, url))

        if not retry and response.status_code == 429:
            logger.info('This Responsys API call got rate limited: {} {}.'.format(method, url))
            time.sleep(self.RESPONSYS_RATE_LIMIT_WAITING_PERIOD_IN_SECONDS)

            response = self._send_request(method, url, headers=headers, retry=True)

        return response

    def send_merge_request(self, method, path, member_field_names, member_records):
        if len(member_records) > self.RECORD_PROCESS_LIMIT_QUANTITY:
            raise ResponsysAPIError('A max of 200 members may be created or updated at one time.')

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
                    "matchColumnName1": "customer_id_",
                    "matchColumnName2": None,
                    "matchOperator": "NONE",
                    "optoutValue": "O",
                    "rejectRecordIfChannelEmpty": None,
                    "defaultPermissionStatus": "OPTIN"
                }
               }

        return self.send_request(method, path, json=json)

    def send_ext_table_merge_request(self, method, path, member_field_names, member_records):
        if len(member_records) > self.RECORD_PROCESS_LIMIT_QUANTITY:
            raise ResponsysAPIError('A max of 200 members may be created or updated at one time.')

        json = {
                "recordData": {
                    "fieldNames": member_field_names,
                    "records": member_records,
                    "mapTemplateName": None
                },
                "insertOnNoMatch": True,
                "updateOnMatch": "REPLACE_ALL",
                "matchColumnName1": "CUSTOMER_ID"
        }

        return self.send_request(method, path, json=json)

    @staticmethod
    def _check_for_valid_response(method, path, response_status_code, response_text,
                                  expected_status_code=200):
        if response_status_code != expected_status_code:
            raise ResponsysAPIError('There was an issue sending a request to Responsys. '
                                    'Request Method: {}, Request Path: {}, '
                                    'Response Status Code: {}. Response Text: {}.'
                                    .format(method, path, response_status_code, response_text))

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
        path = self.AUTH_PATH
        url = urljoin(self.login_url, path)
        params = {'user_name': self.username,
                  'password': self.password,
                  'auth_type': 'password'}
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        response = self._send_request(method, url, params=params, headers=headers)

        if response.status_code != 200:
            raise ResponsysAPIError('There was an issue sending a login request '
                                    'to Responsys. Status Code: {}. Response Text: {}'
                                    .format(response.status_code, response.text))

        self._update_access_items(response)

    def _refresh_token(self):
        method = 'POST'
        path = self.AUTH_PATH
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
