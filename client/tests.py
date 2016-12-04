import json
from datetime import datetime
from datetime import timedelta
from unittest import TestCase

import pytz
import requests
from mock import patch

from client import ResponsysClient
from client import ResponsysClientError


class MockResponseBase(object):

    class MockRequest(object):
        body = ''

    def json(self):
        return {}

    @property
    def request(self):
        return self.MockRequest()


class MockResponse200(MockResponseBase):
    status_code = 200
    text = 'OK'


class MockResponse400(MockResponseBase):
    status_code = 400
    text = ('{"type":"","title":"Invalid username or password","errorCode":'
            '"INVALID_USER_NAME_PASSWORD","detail":"Login failed","errorDetails":[]}')


class MockResponse401(MockResponseBase):
    status_code = 401
    text = ('{"type":"","title":"Authentication token expired","errorCode":'
            '"TOKEN_EXPIRED","detail":"Token expired","errorDetails":[]}')


class MockResponseRateLimit429(MockResponseBase):
    status_code = 429
    text = 'you got rate limited'


class MockResponseInvalidToken500(MockResponseBase):
    status_code = 500
    text = ('{"type":"","title":"Unexpected exception","errorCode":"UNEXPECTED_EXCEPTION",'
            '"detail":"Not a valid authentication token","errorDetails":[]}')

    def json(self):
        return json.loads(self.text)


class MockResponseGeneric500(MockResponseBase):
    status_code = 500
    text = ('{"type":"","title":"Unexpected exception","errorCode":"UNEXPECTED_EXCEPTION",'
            '"detail":"Error on the backend","errorDetails":[]}')

    def json(self):
        return json.loads(self.text)


class ResponsysClientTests(TestCase):

    @staticmethod
    def get_mock_auth_success_response_200(auth_token, issued_endpoint, timestamp_js):
        class MockResponse(MockResponseBase):
            status_code = 200

            def json(self):
                return {u'authToken': auth_token,
                        u'endPoint': issued_endpoint,
                        u'issuedAt': timestamp_js}

        return MockResponse()

    def blank_api_state(self, api):
        self.assertIsNone(api.auth_token)
        self.assertIsNone(api.issued_url)
        self.assertIsNone(api.refresh_timestamp)

    def test_login_success(self):
        auth_token = u'E1sB4M4PSXHCDuXwnzJeGoiHo2RSs'
        issued_endpoint = u'https://api2-018.responsys.net'
        timestamp_js = 1476401899277
        timestamp_datetime = datetime(2016, 10, 13, 23, 38, 19, tzinfo=pytz.utc)

        api = ResponsysClient()
        self.blank_api_state(api)

        with patch.object(requests, 'request') as mock_request:
            mock_request.return_value = self.get_mock_auth_success_response_200(auth_token,
                                                                                issued_endpoint,
                                                                                timestamp_js)
            result = api._login()
            self.assertIsNone(result)

        self.assertEqual(api.auth_token, auth_token)
        self.assertEqual(api.issued_url, issued_endpoint)
        self.assertEqual(api.refresh_timestamp, timestamp_datetime)

    def test_login_failure(self):
        api = ResponsysClient()
        self.blank_api_state(api)

        with patch.object(requests, 'request') as mock_request:
            mock_request.return_value = MockResponse400()

            with self.assertRaises(ResponsysClientError):
                result = api._login()

        self.blank_api_state(api)

    def test_login_failure_continuous_rate_limiting(self):
        api = ResponsysClient()
        self.blank_api_state(api)

        api.RESPONSYS_RATE_LIMIT_WAITING_PERIOD_IN_SECONDS = 0

        with patch.object(requests, 'request') as mock_request:
            mock_request.return_value = MockResponseRateLimit429()
            with self.assertRaises(ResponsysClientError):
                result = api._login()

        self.blank_api_state(api)

    def test_refresh_token_success(self):
        auth_token = u'E1sB4M4PSXHCDuXwnzJeGoiHo2RSs'
        issued_endpoint = u'https://api2-018.responsys.net'
        timestamp_js = 1476401899277
        timestamp_datetime = datetime(2016, 10, 13, 23, 38, 19, tzinfo=pytz.utc)

        api = ResponsysClient()
        self.blank_api_state(api)

        with patch.object(requests, 'request') as mock_request:
            mock_request.return_value = self.get_mock_auth_success_response_200(auth_token,
                                                                                issued_endpoint,
                                                                                timestamp_js)
            result = api._refresh_token()
            self.assertIsNone(result)

        self.assertEqual(api.auth_token, auth_token)
        self.assertEqual(api.issued_url, issued_endpoint)
        self.assertEqual(api.refresh_timestamp, timestamp_datetime)

    def test_refresh_token_success_expired_token(self):
        auth_token = u'E1sB4M4PSXHCDuXwnzJeGoiHo2RSs'
        issued_endpoint = u'https://api2-018.responsys.net'
        timestamp_js = 1476401899277
        timestamp_datetime = datetime(2016, 10, 13, 23, 38, 19, tzinfo=pytz.utc)

        api = ResponsysClient()
        self.blank_api_state(api)

        response_1 = MockResponse401()
        response_2 = self.get_mock_auth_success_response_200(auth_token, issued_endpoint,
                                                             timestamp_js)
        response_sequence = [response_1, response_2]

        with patch.object(requests, 'request') as mock_request:
            mock_request.side_effect = response_sequence

            result = api._refresh_token()

            self.assertEqual(mock_request.call_count, 2)

        self.assertEqual(api.auth_token, auth_token)
        self.assertEqual(api.issued_url, issued_endpoint)
        self.assertEqual(api.refresh_timestamp, timestamp_datetime)

    def test_refresh_token_success_invalid_token(self):
        auth_token = u'E1sB4M4PSXHCDuXwnzJeGoiHo2RSs'
        issued_endpoint = u'https://api2-018.responsys.net'
        timestamp_js = 1476401899277
        timestamp_datetime = datetime(2016, 10, 13, 23, 38, 19, tzinfo=pytz.utc)

        api = ResponsysClient()
        self.blank_api_state(api)

        response_1 = MockResponseInvalidToken500()
        response_2 = self.get_mock_auth_success_response_200(auth_token, issued_endpoint,
                                                             timestamp_js)
        response_sequence = [response_1, response_2]

        with patch.object(requests, 'request') as mock_request:
            mock_request.side_effect = response_sequence

            result = api._refresh_token()

            self.assertEqual(mock_request.call_count, 2)

        self.assertEqual(api.auth_token, auth_token)
        self.assertEqual(api.issued_url, issued_endpoint)
        self.assertEqual(api.refresh_timestamp, timestamp_datetime)

    def test_refresh_token_failure_double_500(self):
        api = ResponsysClient()
        self.blank_api_state(api)

        with patch.object(requests, 'request') as mock_request:
            mock_request.return_value = MockResponseInvalidToken500()

            with self.assertRaises(ResponsysClientError):
                result = api._refresh_token()

        self.blank_api_state(api)

    def test_time_to_refresh_true(self):
        now = datetime.utcnow().replace(tzinfo=pytz.utc)

        api = ResponsysClient()
        api.auth_token = '34oiuh4f9j34f4v3'
        api.refresh_timestamp = now - timedelta(hours=1, minutes=1)

        self.assertTrue(api._time_to_refresh_token())

    def test_time_to_refresh_false(self):
        now = datetime.utcnow().replace(tzinfo=pytz.utc)

        api = ResponsysClient()
        api.auth_token = '34oiuh4f9j34f4v3'
        api.refresh_timestamp = now - timedelta(minutes=59)

        self.assertFalse(api._time_to_refresh_token())

    def test_get_access_items_login_success(self):
        auth_token = u'E1sB4M4PSXHCDuXwnzJeGoiHo2RSs'
        issued_endpoint = u'https://api2-018.responsys.net'
        timestamp_js = 1476401899277
        timestamp_datetime = datetime(2016, 10, 13, 23, 38, 19, tzinfo=pytz.utc)

        api = ResponsysClient()
        self.blank_api_state(api)

        with patch.object(requests, 'request') as mock_request:
            mock_request.return_value = self.get_mock_auth_success_response_200(auth_token,
                                                                                issued_endpoint,
                                                                                timestamp_js)
            api._get_access_items()

        self.assertEqual(api.auth_token, auth_token)
        self.assertEqual(api.issued_url, issued_endpoint)
        self.assertEqual(api.refresh_timestamp, timestamp_datetime)

    def test_except_timeout_exception(self):
        api = ResponsysClient()
        self.blank_api_state(api)

        with patch.object(requests, 'request') as mock_request:
            mock_request.side_effect = requests.exceptions.Timeout()

            with self.assertRaises(ResponsysClientError):
                api._login()

        self.blank_api_state(api)

    def test_except_generic_exception(self):
        api = ResponsysClient()
        self.blank_api_state(api)

        with patch.object(requests, 'request') as mock_request:
            mock_request.side_effect = Exception()

            with self.assertRaises(ResponsysClientError):
                api._login()

        self.blank_api_state(api)

    def test_get_profile_lists_failure_generic_500(self):
        api = ResponsysClient()
        self.blank_api_state(api)

        response = MockResponseGeneric500()

        with patch.object(requests, 'request') as mock_request:
            mock_request.return_value = response

            with self.assertRaises(ResponsysClientError):
                result = api.get_profile_lists()

            self.assertEqual(mock_request.call_count, 1)

    def test_get_profile_contact_lists_success_initial_invalid_token(self):
        auth_token = u'E1sB4M4PSXHCDuXwnzJeGoiHo2RSs'
        issued_endpoint = u'https://api2-018.responsys.net'
        timestamp_js = 1476401899277

        api = ResponsysClient()
        self.blank_api_state(api)
        api.auth_token = 'cpowdij34023'
        api.refresh_timestamp = datetime(2016, 10, 13, 23, 38, 19, tzinfo=pytz.utc)

        response_1 = MockResponseInvalidToken500()
        response_2 = self.get_mock_auth_success_response_200(auth_token, issued_endpoint,
                                                             timestamp_js)
        response_3 = MockResponse200()

        response_sequence = [response_1, response_2, response_3]

        with patch.object(requests, 'request') as mock_request:
            mock_request.side_effect = response_sequence

            result = api.get_profile_lists()

            self.assertEqual(mock_request.call_count, 3)

    def test_is_invalid_token_response(self):
        api = ResponsysClient()
        response = MockResponseInvalidToken500()
        self.assertTrue(api._is_invalid_token_response(response))
