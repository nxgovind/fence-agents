import unittest
from unittest.mock import patch, MagicMock
from fence_nutanix_ahv import (
    NutanixV4Client,
    connect,
    get_list,
    get_power_status,
    set_power_status,
    power_cycle,
    AHVFenceAgentException,
    InvalidArgsException
)

UUID = '51fdd247-4c27-4bad-aeb9-3cfa269cc826'


class TestNutanixV4Client(unittest.TestCase):
    def setUp(self):
        self.client = NutanixV4Client(
            host="test-host",
            username="username",
            password="password",
            verify=False,
            disable_warnings=True
        )

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_all_vms(self, mock_session):
        mock_session.return_value.request.return_value.json.return_value = {
            'data': [{'name': 'test-vm', 'extId': UUID, 'powerState': 'ON'}]
        }
        mock_session.return_value.request.return_value.status_code = 200
        print("Testing _get_all_vms")
        result = self.client._get_all_vms()
        self.assertEqual(result['data'][0]['name'], 'test-vm')

    @patch.object(NutanixV4Client, '_get_all_vms')
    def test_list_vms(self, mock_get_all_vms):
        mock_get_all_vms.return_value = {
            'data': [
               {'name': 'test-vm1', 'extId': UUID, 'powerState': 'ON'},
               {'name': 'test-vm2', 'extId': UUID, 'powerState': 'OFF'}
            ]
        }

        output = {
            'test-vm1': (UUID, 'ON'),
            'test-vm2': (UUID, 'OFF')
        }

        print("Testing list_vms()")
        result = self.client.list_vms()
        self.assertEqual(result['test-vm1'], (UUID, 'ON'))

    @patch.object(NutanixV4Client, '_get_all_vms')
    def test_empty_list_vms(self, mock_get_all_vms):
        mock_get_all_vms.return_value = {
            'data': []
        }

        print("Testing empty _get_all_vms()")
        result = self.client.list_vms()
        self.assertEqual(result, {})

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_all_vms_filter(self, mock_get_all_vms):
        mock_get_all_vms.return_value.request.return_value.json.return_value = {
            'data': [
               {'name': 'test-vm2', 'extId': UUID, 'powerState': 'ON'},
            ]
        }

        mock_get_all_vms.return_value.request.return_value.status_code = 200
        print("Testing list_vms with filter for test-vm2")
        result = self.client._get_all_vms(filter_str="name eq 'test-vm2'")
        self.assertEqual(result['data'][0]['name'], 'test-vm2')

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_vm_uuid(self, mock_session):
        mock_session.return_value.request.return_value.json.return_value = {
            'data': [{'name': 'test-vm', 'extId': UUID}]
        }

        mock_session.return_value.request.return_value.status_code = 200
        print("Testing _get_vm_uuid()")
        vm_uuid = self.client._get_vm_uuid("test-vm")
        self.assertEqual(vm_uuid, UUID)

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_vm_uuid_invalid(self, mock_session):
        mock_session.return_value.request.return_value.json.return_value = {
            'data': [{'name': 'test-vm', 'extId': UUID}]
        }

        mock_session.return_value.request.return_value.status_code = 200
        print("Testing _get_vm_uuid() with invalid vm")
        vm_uuid = self.client._get_vm_uuid("non-existent-vm")
        self.assertEqual(vm_uuid, None)

    @patch('fence_nutanix_ahv.requests.Session')
    def test_get_power_state(self, mock_session):
        mock_session.return_value.request.return_value.json.side_effect = [
            {'data': [{'name': 'test-vm', 'extId': UUID}]},
            {'data': {'extId': UUID, 'powerState': 'OFF'}},
        ]

        mock_session.return_value.request.return_value.status_code = 200
        print("Testing get_power_state()")
        self.client.get_power_state(vm_name="test-vm")
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
        print("Testing set_power_state()")
        self.client.set_power_state(vm_name="test-vm", power_state="on")
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
        print("Testing power_cycle_vm()")
        self.client.power_cycle_vm(vm_name="test-vm")
        mock_session.return_value.request.assert_called()


class TestFenceNutanixAHV(unittest.TestCase):

    def setUp(self):
        # Set up a mock options dictionary for testing
        self.mock_options = {
            "--ip": "127.0.0.1",
            "--username": "admin",
            "--password": "password",
            "--filter": None,
            "--limit": None,
            "--uuid": None,
            "--plug": "test-vm",
            "--action": "off",
            "--power-timeout": None,
        }

        self.client = NutanixV4Client(
            host="test-host",
            username="username",
            password="password",
            verify=False,
            disable_warnings=True
        )

    @patch("fence_nutanix_ahv.NutanixV4Client")
    def test_connect_success(self, mock_client):
        # Mock successful connection
        mock_instance = mock_client.return_value
        mock_instance.list_vms.return_value = {}
        print("Testing connect()")
        client = connect(self.mock_options)
        mock_instance.list_vms.assert_called_once_with(limit=1)
        self.assertEqual(client, mock_instance)

    @patch.object(NutanixV4Client, "list_vms")
    def test_get_list(self, mock_list_vms):
        mock_list_vms.return_value = {
            'test-vm1': (UUID, 'ON'),
            'test-vm2': (UUID, 'OFF')
        }

        print("Testing get_list()")
        result = get_list(self.client, self.mock_options)
        self.assertIn("test-vm1", result)
        self.assertIn("test-vm2", result)
        mock_list_vms.assert_called_once_with(None, None)

    @patch.object(NutanixV4Client, 'get_power_state')
    def test_get_power_status(self, mock_get_power_state):
        mock_get_power_state.return_value = 'on'
        options = {"--uuid": UUID}
        print("Testing get_power_status()")
        result = get_power_status(self.client, self.mock_options)
        self.assertEqual(result, 'on')

    @patch.object(NutanixV4Client, 'set_power_state')
    def test_set_power_status(self, mock_set_power_state):
        plug = self.mock_options["--plug"]
        print("Testing set_power_status()")
        set_power_status(self.client, self.mock_options)
        mock_set_power_state.assert_called_with(vm_name=plug, vm_uuid=None,
                                                power_state="off", timeout=None)

    @patch.object(NutanixV4Client, 'power_cycle_vm')
    def test_power_cycle(self, mock_power_cycle_vm):
        plug = self.mock_options["--plug"]
        print("Testing power_cycle()")
        power_cycle(self.client, self.mock_options)
        mock_power_cycle_vm.assert_called_with(vm_name=plug, vm_uuid=None,
                                               timeout=None)


if __name__ == '__main__':
    unittest.main()
