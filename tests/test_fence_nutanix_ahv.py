import unittest
from unittest.mock import patch, MagicMock
from fence_nutanix_ahv import NutanixV4Client, connect, get_list, get_power_status, set_power_status, power_cycle, AHVFenceAgentException, InvalidArgsException

UUID = '51fdd247-4c27-4bad-aeb9-3cfa269cc826'

class TestNutanixV4Client(unittest.TestCase):

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_all_vms(self, mock_session):
        mock_session.return_value.request.return_value.json.return_value = {
            'data': [{'name': 'test-vm', 'extId': UUID, 'powerState': 'ON'}]
        }
        mock_session.return_value.request.return_value.status_code = 200
        client = NutanixV4Client(host="test_host", username="user", password="pass")
        print("Testing _get_all_vms")
        result = client._get_all_vms()
        self.assertEqual(result['data'][0]['name'], 'test-vm')

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_vm_uuid(self, mock_session):
        mock_session.return_value.request.return_value.json.return_value = {
            'data': [{'name': 'test-vm', 'extId': UUID}]
        }
        mock_session.return_value.request.return_value.status_code = 200
        client = NutanixV4Client(host="test_host", username="user", password="pass")
        print("Testing _get_vm_uuid()")
        vm_uuid = client._get_vm_uuid("test-vm")
        self.assertEqual(vm_uuid, UUID)

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_power_state(self, mock_session):
        mock_session.return_value.request.return_value.json.side_effect = [
            {'data': [{'name': 'test-vm', 'extId': UUID}]},
            {'data': {'extId': UUID, 'powerState': 'OFF'}},
        ]
        mock_session.return_value.request.return_value.status_code = 200
        client = NutanixV4Client(host="test_host", username="user", password="pass")
        print("Testing get_power_state()")
        client.get_power_state(vm_name="test-vm")
        mock_session.return_value.request.assert_called()

    @patch('fence_nutanix_ahv.requests.Session')
    def test_set_power_state(self, mock_session):
        mock_session.return_value.request.return_value.json.side_effect = [
            {'data': [{'name': 'test-vm', 'extId': UUID}]},
            {'data': {'extId': UUID, 'powerState': 'OFF'}},
            {'data': {'extId': UUID, 'status': 'SUCCEEDED'}},
            {'data': {'extId': UUID, 'status': 'SUCCEEDED'}},
            {'data': {'extId': UUID, 'status': 'SUCCEEDED'}},
            {'data': {'extId': UUID}}
        ]
        mock_session.return_value.request.return_value.status_code = 200
        client = NutanixV4Client(host="test_host", username="user", password="pass")
        print("Testing set_power_state()")
        client.set_power_state(vm_name="test-vm", power_state="on")
        mock_session.return_value.request.assert_called()

    @patch('fence_nutanix_ahv.requests.Session')
    def test_power_cycle_vm(self, mock_session):
        mock_session.return_value.request.return_value.json.side_effect = [
            {'data': [{'name': 'test-vm', 'extId': UUID}]},
            {'data': {'extId': UUID}},
            {'data': {'status': 'SUCCEEDED'}},
            {'data': {'extId': UUID, 'powerState': 'ON'}},
            {'data': {'extId': UUID}}
        ]

        mock_session.return_value.request.return_value.status_code = 200
        client = NutanixV4Client(host="test_host", username="user", password="pass")
        print("Testing power_cycle_vm()")
        client.power_cycle_vm(vm_name="test-vm")
        mock_session.return_value.request.assert_called()


if __name__ == '__main__':
    unittest.main()
