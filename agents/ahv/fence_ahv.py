"""
AHV Fence agent

Compatible with AHV v3 and v4 API
"""


import logging
import sys
import time
import uuid
import requests

sys.path.append("@FENCEAGENTSLIBDIR@")
import fencing


class NutanixClientException(Exception):
        """
        Exception class for Nutanix NutanixClient
        """

class AHVFenceAgentException(Exception):
        """
        Exception class for AHV Fence Agent
        """


class NutanixClient:
        """
        Nutanix REST Client
        """
        def __init__(self, username, password):
                self.username = username
                self.password = password
                self.valid_status_codes = [200, 202]

        def request(self, url, method='GET', headers=None, **kwargs):
                """
                Process requests based on the requested method.

                Args:
                url(str): URL of the API end point.
                method(str): HTTP request method.
                headers(dict): HTTP request headers
                **kwargs: Any other keyword args relevant to the method.

                Returns:
                response: response object.
                """
                requests.packages.urllib3.disable_warnings()
                session = requests.Session()
                session.auth = (self.username, self.password)

                if headers:
                        session.headers.update(headers)

                response = None

                try:
                        response = session.request(method, url, **kwargs)
                        response.raise_for_status()
                except requests.exceptions.RequestException as err:
                        logging.info("API call failed: %s", err)
                        raise NutanixClientException(f"API call failed: {err}") from err
                if response.status_code not in self.valid_status_codes:
                        logging.info("API call returned status code %s", response.status_code)
                        raise NutanixClientException(f"API call failed: {response}")

                return response


class V4NutanixClient(NutanixClient):
        """
        V4 Client
        """
        def __init__(self, host=None, username=None, password=None, verify=False):
                self.version = '4.0.b1'
                self.host = host
                self.port = 9440
                self.username = username
                self.password = password
                self.verify = verify
                self.base_url = f"https://{self.host}:{self.port}/api"
                self.vm_url = f"{self.base_url}/vmm/v{self.version}/ahv/config/vms"
                self.task_url = f"{self.base_url}/prism/v{self.version}/config/tasks"
                super().__init__(username, password)
                self.vm_list = []

        def _get_headers(self, vm_uuid=None):
                """
                Get headers for HTTP request.

                Args:
                vm_uuid(str): VM UUID.

                Returns:
                dict: HTTP headers.
                """
                resp = None
                headers = {'Accept':'application/json',
                           'Content-Type': 'application/json'}

                if vm_uuid:
                        try:
                                resp = self._get_vm(vm_uuid)
                        except NutanixClientException as err:
                                logging.error("Unable to retrieve etag")
                                raise AHVFenceAgentException from err

                        etag_str = resp.headers['Etag']
                        request_id = str(uuid.uuid1())
                        headers['If-Match'] = etag_str
                        headers['Ntnx-Request-Id'] = request_id

                return headers

        def _get_all_vms(self, filter_str=None):
                """
                Get a list all registered VMs.

                Args:
                filter_str(str): filter string.

                Returns:
                str: A json formatted output of details of all VMs.
                """
                vm_url = self.vm_url
                # Fix this
                if filter_str:
                        vm_url = f"{vm_url}?{filter_str}"
                logging.info("Sending GET request to get VM details, %s", vm_url)
                header_str = self._get_headers()

                try:
                        resp = self.request(url=vm_url, method='GET',
                                            headers=header_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Unable to retrieve VM info")
                        raise AHVFenceAgentException from err

                vms = resp.json()['data']
                return vms

        def _get_vm_uuid(self, vm_name):
                """
                Get VM UUID given a VM name

                Args:
                    vm_name(str): VM name

                Returns:
                    str: VM UUID
                """
                vm_uuid = None

                try:
                        resp = self._get_all_vms()
                except AHVFenceAgentException as err:
                        logging.error("Failed to get VM info for VM %s", vm_name)
                        raise AHVFenceAgentException from err

                for vm in resp['data']:
                        if vm['name'] == vm_name:
                                vm_uuid = vm['extId']
                                break

                return vm_uuid

        def _get_vm(self, vm_uuid):
                """
                Get config details of an existing VM.

                Args:
                vm_uuid(str): VM UUID.

                Returns:
                object: HTTP response object.
                """
                vm_url = self.vm_url + f"/{vm_uuid}"
                logging.info("Sending GET request to get VM details, %s", vm_url)
                header_str = self._get_headers()

                try:
                        resp = self.request(url=vm_url, method='GET',
                                            headers=header_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Failed to retrieve VM details")
                        raise AHVFenceAgentException from err

                return resp

        def _power_off_vm(self, vm_uuid):
                """
                Force hard power off of a VM

                Args:
                vm_uuid(str): VM UUID string

                Returns:
                object: HTTP response object.
                """
                resp = None
                vm_url = self.vm_url + f"/{vm_uuid}/$actions/power-off"
                logging.info("Sending request to power off VM, %s", vm_url)

                try:
                        headers_str = self._get_headers(vm_uuid)
                        resp = self.request(url=vm_url, method='POST',
                                            headers=headers_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Failed to power off VM %s", vm_uuid)
                        raise AHVFenceAgentException from err

                return resp

        def _power_on_vm(self, vm_uuid):
                """
                Power on a VM

                Args:
                vm_uuid(str): VM UUID string

                Returns:
                object: HTTP response object.
                """
                resp = None
                vm_url = self.vm_url + f"/{vm_uuid}/$actions/power-on"
                logging.info("Sending request to power on VM, %s", vm_url)

                try:
                        header_str = self._get_headers(vm_uuid)
                        resp = self.request(url=vm_url, method='POST',
                                            headers=header_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Failed to power on VM %s", vm_uuid)
                        raise AHVFenceAgentException from err

                return resp

        def _wait_for_task(self, task_uuid, timeout=60):
                """
                Wait for task completion

                Args:
                    task_uuid(str): Task UUID
                    timeout(int): Timeout value to wait before failing

                Returns:
                    None
                """
                task_url = f"{self.task_url}/{task_uuid}"
                header_str = self._get_headers()
                task_resp = None
                interval = 10
                try:
                        task_resp = self.request(url=task_url, method='GET',
                                                 headers=header_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Unable to retrieve task status")
                        raise AHVFenceAgentException from err

                task_status = task_resp.json()['data']['status']

                while task_status != 'SUCCEEDED':
                        if task_status == 'FAILED':
                                raise NutanixClientException(f"Task failed, task uuid: {task_uuid}")

                        time.sleep(interval)
                        try:
                                task_resp = self.request(url=task_url, method='GET',
                                                         headers=header_str, verify=self.verify)
                        except NutanixClientException as err:
                                logging.error("Unable to retrieve task status")
                                raise AHVFenceAgentException from err

                        task_status = task_resp.json()['data']['status']
                        timeout = timeout - interval

                        if timeout <= 0:
                                raise AHVFenceAgentException("Timed out waiting"
                                                             f" for task: {task_uuid}")

        def list_vms(self, filter_str=None):
                """
                List all VM's name, UUID, and power state

                Args:
                filter_str(str): filter string.

                Returns:
                list: A list of all VMs (Name, UUID, power state).
                """
                vms = self._get_all_vms(filter_str)
                vm_list = []
                for vm in vms:
                        vm_name = vm['name']
                        ext_id = vm['extId']
                        power_state = vm['powerState']
                        vm_data = f"{vm_name},{ext_id},{power_state}"
                        vm_list.append(vm_data)

                return vm_list

        def get_power_state(self, vm_name=None, vm_uuid=None):
                """
                Get power state of a VM.

                Args:
                task_uuid(str): Task UUID.

                Returns:
                str: A json formatted response to the HTTP request.
                """
                resp = None

                if not vm_name and not vm_uuid:
                        logging.error("Require at least one of VM name or VM UUID")
                        raise AHVFenceAgentException("No arguments provided")

                if not vm_uuid:
                        try:
                                vm_uuid = self._get_vm_uuid(vm_name)
                        except AHVFenceAgentException as err:
                                logging.error("Unable to retrieve UUID of VM, %s", vm_name)
                                raise AHVFenceAgentException from err

                try:
                        resp = self._get_vm(vm_uuid)
                except AHVFenceAgentException as err:
                        logging.error("Unable to retrieve VM power state")
                        raise AHVFenceAgentException from err

                power_state = resp.json()['data']['powerState']
                return power_state

        def set_power_state(self, vm_name=None, vm_uuid=None, power_state='off'):
                """
                Set power state
                """
                resp = None
                status = None

                if not vm_name and not vm_uuid:
                        logging.error("Require at least one of VM name or VM UUID")
                        raise AHVFenceAgentException("No arguments provided")

                if not vm_uuid:
                        vm_uuid = self._get_vm_uuid(vm_name)

                if power_state.lower() == 'on':
                        resp = self._power_on_vm(vm_uuid)
                elif power_state.lower() == 'off':
                        resp = self._power_off_vm(vm_uuid)

                task_id = resp.json()['data']['extId']

                try:
                        self._wait_for_task(task_id)
                except AHVFenceAgentException as err:
                        logging.error("Failed to power %s VM", power_state.lower())
                        logging.error("VM power %s task failed with status, %s",
                                      power_state.lower(), status)
                        raise AHVFenceAgentException from err

                logging.info("Powered %s VM, %s", power_state.lower(), vm_name)


def connect(options):
        """
        Create a client instance and return it after verifying
        that the client can connect.
        """
        host = options["--ip"]
        username = options["--username"]
        password = options["--password"]
        verify_ssl = False

        if "--ssl-secure" in options:
                verify_ssl = True

        client = V4NutanixClient(host, username, password, verify_ssl)

        try:
                client.list_vms()
        except AHVFenceAgentException as err:
                logging.error("Connection to Prism Central Failed")
                logging.error(err)
                fencing.fail(fencing.EC_LOGIN_DENIED)

        return client

def list_vms(client, options):
        """
        List VMs
        """

        vm_list = None

        try:
                vm_list = client.list_vms()
        except AHVFenceAgentException as err:
                logging.error("Failed to list VMs")
                logging.error(err)
                fencing.fail(fencing.EC_GENERIC_ERROR)
        return vm_list
