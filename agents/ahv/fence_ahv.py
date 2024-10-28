"""
AHV Fence agent

Compatible with AHV v4 API
"""


import atexit
import logging
import sys
import time
import uuid
import requests

sys.path.append("@FENCEAGENTSLIBDIR@")
import fencing


V4_VERSION = '4.0'
MIN_TIMEOUT = 60
PC_PORT = 9440


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
                session = requests.Session()
                session.auth = (self.username, self.password)

                if headers:
                        session.headers.update(headers)

                response = None

                try:
                        response = session.request(method, url, **kwargs)
                        response.raise_for_status()
                except requests.exceptions.RequestException as err:
                        logging.error("API call failed: %s", response.text)
                        logging.error("Error message: %s", err)
                        raise NutanixClientException(f"API call failed: {err}") from err

                if response.status_code not in self.valid_status_codes:
                        logging.error("API call returned status code %s", response.status_code)
                        raise NutanixClientException(f"API call failed: {response}")

                return response


class V4NutanixClient(NutanixClient):
        """
        Nutanix V4 API client wrapper class. This implements the
        necessary methods for listing VMs, getting power state
        of VMs, and setting power state of VMs.
        """
        def __init__(self, host=None, username=None, password=None, verify=False):
                """
                Init method

                Args:
                    host(str): Host IP address or hostname
                    username(str): username for Prism Central
                    password(str): password for Prism Central account
                    verify(boolean): ssl verify

                Returns:
                    None
                """
                self.host = host
                self.username = username
                self.password = password
                self.verify = verify
                self.base_url = f"https://{self.host}:{PC_PORT}/api"
                self.vm_url = f"{self.base_url}/vmm/v{V4_VERSION}/ahv/config/vms"
                self.task_url = f"{self.base_url}/prism/v{V4_VERSION}/config/tasks"
                super().__init__(username, password)

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
                        except AHVFenceAgentException as err:
                                logging.error("Unable to retrieve etag")
                                raise AHVFenceAgentException from err

                        etag_str = resp.headers['Etag']
                        request_id = str(uuid.uuid1())
                        headers['If-Match'] = etag_str
                        headers['Ntnx-Request-Id'] = request_id

                return headers

        def _get_all_vms(self, filter_str=None, limit=None):
                """
                Get a list all registered VMs in Nutanix Prism Central cluster

                Args:
                filter_str(str): filter string.
                limit(int): Number of VMs to return

                Returns:
                str: A json formatted output of details of all VMs.
                """
                vm_url = self.vm_url

                if filter_str and limit:
                        vm_url = f"{vm_url}?$filter={filter_str}&$limit={limit}"
                elif filter_str and not limit:
                        vm_url = f"{vm_url}?$filter={filter_str}"
                elif limit and not filter_str:
                        vm_url = f"{vm_url}?$limit={limit}"

                logging.info("Sending GET request to get VM details, %s", vm_url)
                header_str = self._get_headers()

                try:
                        resp = self.request(url=vm_url, method='GET',
                                            headers=header_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Unable to retrieve VM info")
                        raise AHVFenceAgentException from err

                vms = resp.json()
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
                resp = None

                if not vm_name:
                        logging.error("VM name was not provided")
                        raise AHVFenceAgentException("VM name not provided")

                try:
                        filter_str = f"name eq '{vm_name}'"
                        resp = self._get_all_vms(filter_str=filter_str)
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
                if not vm_uuid:
                        logging.error("VM UUID was not provided")
                        raise AHVFenceAgentException("VM UUID not provided")

                vm_url = self.vm_url + f"/{vm_uuid}"
                logging.info("Sending GET request to get VM details, %s", vm_url)
                header_str = self._get_headers()

                try:
                        resp = self.request(url=vm_url, method='GET',
                                            headers=header_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Failed to retrieve VM details "
                                      "for VM UUID: vm_uuid")
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

                if not vm_uuid:
                        logging.error("VM UUID was not provided")
                        raise AHVFenceAgentException("VM UUID not provided")

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
                if not vm_uuid:
                        logging.error("VM UUID was not provided")
                        raise AHVFenceAgentException("VM UUID not provided")

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

        def _power_cycle_vm(self, vm_uuid):
                """
                Power cycle a VM

                Args:
                vm_uuid(str): VM UUID string

                Returns:
                object: HTTP response object.
                """
                if not vm_uuid:
                        logging.error("VM UUID was not provided")
                        raise AHVFenceAgentException("VM UUID not provided")

                resp = None
                vm_url = self.vm_url + f"/{vm_uuid}/$actions/power-cycle"
                logging.info("Sending request to power cycle VM, %s", vm_url)

                try:
                        header_str = self._get_headers(vm_uuid)
                        resp = self.request(url=vm_url, method='POST',
                                            headers=header_str, verify=self.verify)
                except NutanixClientException as err:
                        logging.error("Failed to power on VM %s", vm_uuid)
                        raise AHVFenceAgentException from err

                return resp

        def _wait_for_task(self, task_uuid, timeout=None):
                """
                Wait for task completion

                Args:
                    task_uuid(str): Task UUID
                    timeout(int): Timeout value to wait before failing

                Returns:
                    None
                """
                if not task_uuid:
                        logging.error("Task UUID was not provided")
                        raise AHVFenceAgentException("Task UUID not provided")

                task_url = f"{self.task_url}/{task_uuid}"
                header_str = self._get_headers()
                task_resp = None
                interval = 10
                task_status = None

                if not timeout:
                        timeout = MIN_TIMEOUT

                while task_status != 'SUCCEEDED':
                        if task_status == 'FAILED':
                                raise NutanixClientException(f"Task failed, task uuid: {task_uuid}")

                        try:
                                task_resp = self.request(url=task_url, method='GET',
                                                         headers=header_str, verify=self.verify)
                                task_status = task_resp.json()['data']['status']
                        except NutanixClientException as err:
                                logging.error("Unable to retrieve task status")
                                raise AHVFenceAgentException from err

                        if task_status == 'SUCCEEDED':
                                break

                        time.sleep(interval)
                        timeout = timeout - interval

                        if task_status == 'SUCCEEDED':
                                break

                        if timeout <= 0:
                                raise AHVFenceAgentException("Timed out waiting"
                                                             f" for task: {task_uuid}")

        def list_vms(self, filter_str=None, limit=None):
                """
                List all VM's name, UUID, and power state

                Args:
                filter_str(str): filter string.

                Returns:
                list: A list of all VMs (Name, UUID, power state).
                """
                vms = self._get_all_vms(filter_str, limit)
                vm_list = []

                for vm in vms['data']:
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
                        logging.error("Unable to retrieve power state of VM %s", vm_uuid)
                        raise AHVFenceAgentException from err

                power_state = resp.json()['data']['powerState']

                if power_state.lower() != 'on':
                        power_state = 'OFF'

                return power_state

        def set_power_state(self, vm_name=None, vm_uuid=None,
                            power_state='off', timeout=None):
                """
                Set power state of a VM

                Args:
                    vm_name(str): Name of VM
                    vm_uuid(str): VM UUID
                    power_state(str): Requested power state of VM (on/off)
                    timeout(int): Timeout for task in seconds

                Returns:
                    None
                """
                resp = None
                status = None
                current_power_state = None

                if not timeout:
                        timeout = MIN_TIMEOUT

                if not vm_name and not vm_uuid:
                        logging.error("Require at least one of VM name or VM UUID")
                        raise AHVFenceAgentException("No arguments provided")

                if not vm_uuid:
                        vm_uuid = self._get_vm_uuid(vm_name)

                try:
                        current_power_state = self.get_power_state(vm_uuid=vm_uuid)
                except AHVFenceAgentException as err:
                        raise AHVFenceAgentException from err

                if current_power_state.lower() == power_state.lower():
                        logging.info("VM already powered %s", power_state.lower())
                        return

                if power_state.lower() == 'on':
                        resp = self._power_on_vm(vm_uuid)
                elif power_state.lower() == 'off':
                        resp = self._power_off_vm(vm_uuid)

                task_id = resp.json()['data']['extId']

                try:
                        self._wait_for_task(task_id, timeout)
                except AHVFenceAgentException as err:
                        logging.error("Failed to power %s VM", power_state.lower())
                        logging.error("VM power %s task failed with status, %s",
                                      power_state.lower(), status)
                        raise AHVFenceAgentException from err

                logging.info("Powered %s VM, %s successfully",
                             power_state.lower(), vm_name)

        def power_cycle_vm(self, vm_name=None, vm_uuid=None, timeout=None):
                """
                Power cycle a VM

                 Args:
                    vm_name(str): Name of VM
                    vm_uuid(str): VM UUID
                    timeout(int): Timeout for task in seconds

                Returns:
                    None
                """
                resp = None
                status = None

                if not timeout:
                        timeout = MIN_TIMEOUT

                if not vm_name and not vm_uuid:
                        logging.error("Require at least one of VM name or VM UUID")
                        raise AHVFenceAgentException("No arguments provided")

                if not vm_uuid:
                        vm_uuid = self._get_vm_uuid(vm_name)

                resp = self._power_cycle_vm(vm_uuid)
                task_id = resp.json()['data']['extId']

                try:
                        self._wait_for_task(task_id, timeout)
                except AHVFenceAgentException as err:
                        logging.error("Failed to power-cycle VM %s", vm_uuid)
                        logging.error("VM power-cycle task failed with status, %s", status)
                        raise AHVFenceAgentException from err

                logging.info("Power-cycled VM, %s", vm_name)


def connect(options):
        """
        Create a client instance and return it after verifying
        that the client can connect.

        Args:
            options(dict): CLI options dictionary

        Returns:
            A NutanixClient instance
        """
        host = options["--ip"]
        username = options["--username"]
        password = options["--password"]
        verify_ssl = False

        if "--ssl-secure" in options:
                verify_ssl = True

        client = V4NutanixClient(host, username, password, verify_ssl)

        try:
                client.list_vms(limit=1)
        except AHVFenceAgentException as err:
                logging.error("Connection to Prism Central Failed")
                logging.error(err)
                fencing.fail(fencing.EC_LOGIN_DENIED)

        return client

def get_list(client, options):
        """
        List VMs registered with Prism Central

        Args:
            client(NutanixClient): Nutanix client instance
            options(dict): CLI options dictionary

        Returns:
            List of registered VMs
        """

        vm_list = None
        limit = None
        filter_str =  None
        display_list = None

        if "--filter" in options:
                filter_str = options["--filter"]

        if "--limit" in options:
                limit = options["limit"]

        try:
                vm_list = client.list_vms(filter_str, limit)
                display_list = "\n".join(vm_list)
        except AHVFenceAgentException as err:
                logging.error("Failed to list VMs")
                logging.error(err)
                fencing.fail(fencing.EC_GENERIC_ERROR)

        return display_list

def get_power_state(client, options):
        """
        Get power state of a VM

        Args:
            Client(V4NutanixClient instance): V4NutanixClient instance
            options(dict): options dictionary

        Returns:
            None
        """
        vmid = None
        name = None
        power_state = None

        if "--uuid" in options:
                vmid = options["--uuid"]
        else:
                name = options["--plug"]

        if not vmid and not name:
                logging.error("Need VM name or VM UUID for power op")
                fencing.fail(fencing.EC_GENERIC_ERROR)
        try:
                power_state = client.get_power_state(vm_name=name, vm_uuid=vmid)
        except AHVFenceAgentException:
                fencing.fail(fencing.EC_GENERIC_ERROR)

        return power_state

def set_power_state(client, options):
        """
        Set power state of a VM

        Args:
            Client(V4NutanixClient instance): V4NutanixClient instance
            options(dict): options dictionary

        Returns:
            None
        """
        vmid = None
        name = None
        action = None
        timeout = None

        if "--action" not in options:
                logging.error("No power op action specified")
                fencing.fail(fencing.EC_GENERIC_ERROR)

        action = options["--action"].lower()

        if "--power-timeout" in options:
                timeout = options["--power-timeout"]

        if "--uuid" in options:
                vmid = options["--uuid"]
        else:
                name = options["--plug"]

        if not name and not vmid:
                logging.error("Need VM name or VM UUID to set power state of a VM")
                fencing.fail(fencing.EC_GENERIC_ERROR)

        try:
                client.set_power_state(vm_name=name, vm_uuid=vmid,
                                       power_state=action, timeout=timeout)
        except AHVFenceAgentException as err:
                logging.error(err)
                fencing.fail(fencing.EC_GENERIC_ERROR)

def power_cycle(client, options):
        """
        Power cycle a VM

        Args:
            Client(V4NutanixClient instance): V4NutanixClient instance
            options(dict): options dictionary

        Returns:
            None
        """
        vmid = None
        name = None
        timeout = None

        if "--power-timeout" in options:
                timeout = options["--power-timeout"]

        if "--uuid" in options:
                vmid = options["--uuid"]
        else:
                name = options["--plug"]

        if not name and not vmid:
                logging.error("Need VM name or VM UUID to set power cycling a VM")
                fencing.fail(fencing.EC_GENERIC_ERROR)

        try:
                client.power_cycle_vm(vm_name=name, vm_uuid=vmid, timeout=timeout)
        except AHVFenceAgentException as err:
                logging.error(err)
                fencing.fail(fencing.EC_GENERIC_ERROR)

def define_new_opts():
        """
        Define new CLI args
        """
        fencing.all_opt["filter"] = {
                "getopt": ":",
                "longopt": "filter",
                "help": """
                        --filter=[filter]	Filter list, list VMs actions.
                        --filter=\"name eq 'node1-vm'\"
                        --filter=\"startswith(name,'node')\"
                        --filter=\"name in ('node1-vm','node-3-vm')\" """,
                "required": "0",
                "shortdesc": "Filter list, get_list"
                             "e.g: \"name eq 'node1-vm'\"",
                "order": 2
        }

def main():
        """
        Main function
        """
        device_opt = [
            "ipaddr",
            "login",
            "passwd",
            "ssl",
            "notls",
            "web",
            "port",
            "filter",
            "method",
            "disable_timeout",
            "power_timeout"
        ]

        atexit.register(fencing.atexit_handler)
        define_new_opts()

        fencing.all_opt["method"]["default"] = "onoff"
        fencing.all_opt["disable_timeout"]["default"] = "false"
        fencing.all_opt["power_timeout"]["default"] = str(MIN_TIMEOUT)
        options = fencing.check_input(device_opt, fencing.process_input(device_opt))
        docs = {}
        docs["shortdesc"] = "Fencing agent for Nutanix AHV Cluster VMs."
        docs["longdesc"] = """
                            fence_ahv is a fencing agent for nodes
                            deployed on Nutanix AHV cluster with AHV cluster
                            being managed by Prism Central.
                            """
        docs["vendorurl"] = "https://www.nutanix.com"
        fencing.show_docs(options, docs)
        fencing.run_delay(options)
        client = connect(options)

        result = fencing.fence_action(
                client, options, set_power_state, get_power_state,
                get_list, reboot_cycle_fn=power_cycle
                )

        sys.exit(result)


if __name__ == "__main__":
        main()
