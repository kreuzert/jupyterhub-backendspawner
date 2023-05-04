import asyncio
import copy
import json
import re
import uuid
from datetime import datetime

from async_generator import aclosing
from jupyterhub.utils import maybe_future
from jupyterhub.utils import url_path_join
from traitlets import Callable
from traitlets import Dict
from traitlets import Union

from .backendspawner import BackendSpawner


class EventBackendSpawner(BackendSpawner):
    _yielded_events = []
    _cancel_event_yielded = False
    latest_events = []
    events = {}
    clear_events = True
    yield_wait_seconds = 1

    def get_state(self):
        """get the current state"""
        state = super().get_state()
        if self.events:
            if type(self.events) != dict:
                self.events = {}
            self.events["latest"] = self.latest_events
            # Clear logs older than 24h or empty logs
            events_keys = copy.deepcopy(list(self.events.keys()))
            for key in events_keys:
                value = self.events.get(key, None)
                if value and len(value) > 0 and value[0]:
                    stime = self._get_event_time(value[0])
                    dtime = datetime.strptime(stime, "%Y_%m_%d %H:%M:%S")
                    now = datetime.now()
                    delta = now - dtime
                    if delta.days:
                        del self.events[key]
                else:  # empty logs
                    del self.events[key]
            state["events"] = self.events
        return state

    def load_state(self, state):
        """load state from the database"""
        super().load_state(state)
        if "events" in state:
            self.events = state["events"]

    def clear_state(self):
        """clear any state (called after shutdown)"""
        super().clear_state()
        self._cancel_event_yielded = False
        if self.clear_events:
            self.events = {}
            self.clear_events = False

    async def _generate_progress(self):
        """Private wrapper of progress generator

        This method is always an async generator and will always yield at least one event.
        """
        if not self._spawn_pending:
            self.log.warning(
                "Spawn not pending, can't generate progress for %s", self._log_name
            )
            return

        async with aclosing(self.progress()) as progress:
            async for event in progress:
                yield event

    async def progress(self):
        spawn_future = self._spawn_future
        next_event = 0

        break_while_loop = False
        while True:
            # Ensure we always capture events following the start_future
            # signal has fired.
            if spawn_future.done():
                break_while_loop = True

            len_events = len(self.latest_events)
            if next_event < len_events:
                for i in range(next_event, len_events):
                    yield self.latest_events[i]
                    if self.latest_events[i].get("failed", False) == True:
                        self._cancel_event_yielded = True
                        break_while_loop = True
                next_event = len_events

            if break_while_loop:
                break
            await asyncio.sleep(self.yield_wait_seconds)

    def status_update_url(self, server_name=""):
        """API path for status update endpoint for a server with a given name"""
        url_parts = ["users", "progress", "status", self.user.escaped_name]
        if server_name:
            url_parts.append(server_name)
        return url_path_join(*url_parts)

    @property
    def _status_update_url(self):
        return self.status_update_url(self.name)

    def get_env(self):
        env = super().get_env()
        env["JUPYTERHUB_STATUS_URL"] = self._status_update_url
        return env

    def _get_event_time(self, event):
        # Regex for date time
        pattern = re.compile(
            r"([0-9]+(_[0-9]+)+).*[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]{1,3})?"
        )
        message = event["html_message"]
        match = re.search(pattern, message)
        return match.group()

    def get_ready_event(self):
        event = super().get_ready_event()
        ready_msg = f"Service {self.name} started."
        now = datetime.now().strftime("%Y_%m_%d %H:%M:%S.%f")[:-3]
        url = url_path_join(self.user.url, self.name, "/")
        event[
            "html_message"
        ] = f'<details><summary>{now}: {ready_msg}</summary>You will be redirected to <a href="{url}">{url}</a></details>'
        self.latest_events.append(event)
        return event

    cancelling_event = Union(
        [Dict(), Callable()],
        default_value={
            "failed": False,
            "ready": False,
            "progress": 99,
            "message": "",
            "html_message": "JupyterLab is cancelling the start.",
        },
        help="""
        Event shown when single-user server was cancelled.
        """,
    ).tag(config=True)

    async def get_cancelling_event(self):
        if callable(self.cancelling_event):
            cancelling_event = await maybe_future(self.cancelling_event(self))
        else:
            cancelling_event = self.cancelling_event
        return cancelling_event

    stop_event = Union(
        [Dict(), Callable()],
        default_value={
            "failed": True,
            "ready": False,
            "progress": 100,
            "message": "",
            "html_message": "JupyterLab was stopped.",
        },
        help="""
        Event shown when single-user server was stopped.
        """,
    ).tag(config=True)

    async def get_stop_event(self):
        if callable(self.stop_event):
            stop_event = await maybe_future(self.stop_event(self))
        else:
            stop_event = self.stop_event
        return stop_event

    def run_failed_spawn_request_hook(self, exception):
        now = datetime.now().strftime("%Y_%m_%d %H:%M:%S.%f")[:-3]
        event = {
            "progress": 99,
            "failed": False,
            "html_message": f"<details><summary>{now}: JupyterLab start failed. Deleting related resources...</summary>This may take a few seconds.</details>",
        }
        self.latest_events.append(event)
        summary = "Unknown Error"
        details = ""
        if hasattr(exception, "args") and len(exception.args) > 2:
            try:
                error = json.loads(exception.args[2].body.decode())
                if "error" in error.keys() and "detailed_error" in error.keys():
                    summary = error.get("error")
                    details = error.get("detailed_error")
                else:
                    summary = f"{exception.args[0]} - {exception.args[1]}"
                    details = str(error)
            except:
                self.log.exception("Could not load detailed error message")
        elif hasattr(exception, "args") and len(exception.args) > 1:
            summary = f"{exception.args[0]} - {exception.args[1]}"

        async def _get_stop_event(spawner):
            now = datetime.now().strftime("%Y_%m_%d %H:%M:%S.%f")[:-3]
            event = {
                "progress": 100,
                "failed": True,
                "html_message": f"<details><summary>{now}: {summary}</summary>{details}</details>",
            }
            return event

        self.stop_event = _get_stop_event
        return super().run_failed_spawn_request_hook(exception)

    def run_pre_spawn_hook(self):
        """Some commands are required."""
        if self.already_stopped:
            raise Exception("Server is in the process of stopping, please wait.")

        self.start_id = uuid.uuid4().hex[:8]
        if self.internal_ssl:
            service_address = self.get_service_address()
            self.ssl_alt_names += [f"DNS:{service_address}"]

        # Save latest events with start event time
        if self.latest_events != []:
            try:
                start_event = self.latest_events[0]
                start_event_time = self._get_event_time(start_event)
                self.events[start_event_time] = self.latest_events
            except:
                self.log.info(
                    f"Could not retrieve latest_events. Reset events list for {self._log_name}"
                )
                self.latest_events = []
                self.events = {}
        self.latest_events = []
        if type(self.events) != dict:
            self.events = {}
        self.events["latest"] = self.latest_events

        now = datetime.now().strftime("%Y_%m_%d %H:%M:%S.%f")[:-3]
        start_pre_msg = "Sending request to backend service to start your service."
        start_event = {
            "failed": False,
            "progress": 10,
            "html_message": f"<details><summary>{now}: {start_pre_msg}</summary>\
                &nbsp;&nbsp;Start ID: {self.start_id}<br>&nbsp;&nbsp;Options:<br><pre>{json.dumps(self.user_options, indent=2)}</pre></details>",
        }
        self.latest_events = [start_event]

        """Run the pre_spawn_hook if defined"""
        if self.pre_spawn_hook:
            return self.pre_spawn_hook(self)

    def run_post_spawn_request_hook(self, resp_json):
        now = datetime.now().strftime("%Y_%m_%d %H:%M:%S.%f")[:-3]
        submitted_event = {
            "failed": False,
            "ready": False,
            "progress": 30,
            "html_message": f"<details><summary>{now}: Backend communication successful.</summary>You will receive further information about the service status from the service itself.</details>",
        }
        self.latest_events.append(submitted_event)
        return self.post_spawn_request_hook(self, resp_json)

    async def stop(self, now=False, cancel=False, event=None):
        if self.already_stopped:
            return

        if cancel:
            cancelling_event = await self.get_cancelling_event()
            self.latest_events.append(cancelling_event)

        try:
            # always use cancel=False, and call the function later if neccessary.
            # Otherwise the stop_event would be attached after run_post_stop_hook was called.
            await super().stop(now, cancel=False)
        finally:
            if not event:
                event = await self.get_stop_event()
            elif callable(event):
                event = await maybe_future(event(self))
            self.latest_events.append(event)

        if cancel:
            await self.cancel()

