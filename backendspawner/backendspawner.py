import asyncio
import json
import random
import time
import uuid
from pathlib import Path
from urllib.parse import urlparse
from urllib.parse import urlunparse

from jupyterhub.spawner import Spawner
from jupyterhub.utils import maybe_future
from tornado.httpclient import AsyncHTTPClient
from tornado.httpclient import HTTPClientError
from tornado.httpclient import HTTPRequest
from tornado.ioloop import PeriodicCallback
from traitlets import Any
from traitlets import Bool
from traitlets import Callable
from traitlets import default
from traitlets import Dict
from traitlets import Integer
from traitlets import Unicode
from traitlets import Union


class BackendSpawner(Spawner):
    # We will give each Start attempt its own 8 chars long id.
    start_id = ""

    # This is used to prevent multiple requests during the stop procedure.
    already_stopped = False
    already_post_stop_hooked = False

    request_url_start = Union(
        [Unicode(), Callable()],
        help="""
        URL used in single-user server start request.
        """,
    ).tag(config=True)

    async def get_request_url_start(self):
        if callable(self.request_url_start):
            request_url_start = await maybe_future(self.request_url_start(self))
        else:
            request_url_start = self.request_url_start
        return request_url_start

    request_body_start = Union(
        [Dict(), Callable()],
        help="""
        body used in single-user server start request.
        """,
    ).tag(config=True)

    async def get_request_body_start(self):
        if callable(self.request_body_start):
            request_body_start = await maybe_future(self.request_body_start(self))
        else:
            request_body_start = self.request_body_start
        return request_body_start

    request_headers_start = Union(
        [Dict(), Callable()],
        help="""
        headers used in single-user server start request.
        """,
    ).tag(config=True)

    async def get_request_headers_start(self):
        if callable(self.request_headers_start):
            request_headers_start = await maybe_future(self.request_headers_start(self))
        else:
            request_headers_start = self.request_headers_start
        return request_headers_start

    request_kwargs = Union(
        [Dict(), Callable()],
        default_value={},
        help="""
        Allows you to add additional keywords to HTTPRequest Object.
        Examples:
          ca_certs,
          validate_cert,
          request_timeout
        """,
    ).tag(config=True)

    def get_request_kwargs(self):
        if callable(self.request_kwargs):
            request_kwargs = self.request_kwargs(self)
        else:
            request_kwargs = self.request_kwargs
        return request_kwargs

    port = Union(
        [Integer(), Callable()],
        default_value=8080,
        help="""
        """,
    ).tag(config=True)

    async def get_port(self):
        if callable(self.port):
            port = await maybe_future(self.port(self))
        else:
            port = self.port
        return port

    poll_interval = Union(
        [Integer(), Callable()],
        default_value=0,
        help="""
        Interval (in seconds) on which to poll the spawner for single-user server's status.

        At every poll interval, each spawner's `.poll` method is called, which checks
        if the single-user server is still running. If it isn't running, then JupyterHub modifies
        its own state accordingly and removes appropriate routes from the configurable proxy.
        """,
    ).tag(config=True)

    def get_poll_interval(self):
        if callable(self.poll_interval):
            poll_interval = self.poll_interval(self)
        else:
            poll_interval = self.poll_interval
        return poll_interval

    service_address = Union(
        [Unicode(), Callable()],
        help="""
        JupyterHub will look at this address for the single-user server.
        """,
    ).tag(config=True)

    def get_service_address(self):
        if callable(self.service_address):
            service_address = self.service_address(self)
        else:
            service_address = self.service_address
        return service_address

    def run_pre_spawn_hook(self):
        if self.already_stopped:
            raise Exception("Server is in the process of stopping, please wait.")
        self.start_id = uuid.uuid4().hex[:8]
        if self.internal_ssl:
            service_address = self.get_service_address()
            self.ssl_alt_names += [f"DNS:{service_address}"]

        """Run the pre_spawn_hook if defined"""
        if self.pre_spawn_hook:
            return self.pre_spawn_hook(self)

    def run_post_stop_hook(self):
        if self.already_post_stop_hooked:
            return
        self.already_post_stop_hooked = True

        """Run the post_stop_hook if defined"""
        if self.post_stop_hook is not None:
            try:
                return self.post_stop_hook(self)
            except Exception:
                self.log.exception("post_stop_hook failed with exception: %s", self)

    failed_spawn_request_hook = Callable(
        help="""
        If a start of a single-user server fails, you can run additional commands here.
        This allows you to handle a failed start attempt properly, according to
        your specific backend API.
        
        Example:
        ```
        def custom_failed_spawn_request_hook(Spawner, exception_thrown):
            ...
            return
        ```
        """
    ).tag(config=True)

    @default("failed_spawn_request_hook")
    def _failed_spawn_request_hook(self):
        return self._default_failed_spawn_request_hook

    def _default_failed_spawn_request_hook(self, spawner, exception):
        return

    def run_failed_spawn_request_hook(self, exception):
        return self.failed_spawn_request_hook(self, exception)

    post_spawn_request_hook = Callable(
        help="""
        If a start of a single-user server was successful, you can run additional commands here.
        This allows you to handle a successful start attempt properly, according to
        your specific backend API.
        
        Example:
        ```
        def post_spawn_request_hook(Spawner, resp_json):
            ...
            return
        ```
        """
    ).tag(config=True)

    @default("post_spawn_request_hook")
    def _post_spawn_request_hook(self):
        return self._default_post_spawn_request_hook

    def _default_post_spawn_request_hook(self, spawner, resp_json):
        return

    def run_post_spawn_request_hook(self, resp_json):
        return self.post_spawn_request_hook(self, resp_json)

    request_url_poll = Union(
        [Unicode(), Callable()],
        help="""
        URL used in single-user server poll request.
        """,
    ).tag(config=True)

    async def get_request_url_poll(self):
        if callable(self.request_url_poll):
            request_url_poll = await maybe_future(self.request_url_poll(self))
        else:
            request_url_poll = self.request_url_poll
        return request_url_poll

    request_headers_poll = Union(
        [Dict(), Callable()],
        help="""
        headers used in single-user server poll request.
        """,
    ).tag(config=True)

    async def get_request_headers_poll(self):
        if callable(self.request_headers_poll):
            request_headers_poll = await maybe_future(self.request_headers_poll(self))
        else:
            request_headers_poll = self.request_headers_poll
        return request_headers_poll

    request_404_poll_keep_running = Bool(
        default_value=False,
        help="""
        How to handle a 404 response from backend API during single-user poll request.
        """,
    ).tag(config=True)

    request_failed_poll_keep_running = Bool(
        default_value=False,
        help="""
        How to handle a failed request to backend API during single-user poll request.
        """,
    ).tag(config=True)

    request_url_stop = Union(
        [Unicode(), Callable()],
        help="""
        URL used in single-user server stop request.
        """,
    ).tag(config=True)

    async def get_request_url_stop(self):
        if callable(self.request_url_stop):
            request_url_stop = await maybe_future(self.request_url_stop(self))
        else:
            request_url_stop = self.request_url_stop
        return request_url_stop

    request_headers_stop = Union(
        [Dict(), Callable()],
        help="""
        headers used in single-user server stop request.
        """,
    ).tag(config=True)

    async def get_request_headers_stop(self):
        if callable(self.request_headers_stop):
            request_headers_stop = await maybe_future(self.request_headers_stop(self))
        else:
            request_headers_stop = self.request_headers_stop
        return request_headers_stop

    http_client = Any()

    @default("http_client")
    def _default_http_client(self):
        return AsyncHTTPClient(force_instance=True, defaults=dict(validate_cert=False))

    def get_state(self):
        """get the current state"""
        state = super().get_state()
        if self.start_id:
            state["start_id"] = self.start_id
        return state

    def load_state(self, state):
        """load state from the database"""
        super().load_state(state)
        if "start_id" in state:
            self.start_id = state["start_id"]

    def clear_state(self):
        """clear any state (called after shutdown)"""
        super().clear_state()
        self.start_id = ""
        self.already_stopped = False
        self.already_post_stop_hooked = False

    def start_polling(self):
        """Start polling periodically for single-user server's running state.

        Callbacks registered via `add_poll_callback` will fire if/when the server stops.
        Explicit termination via the stop method will not trigger the callbacks.

        """
        poll_interval = self.get_poll_interval()
        if poll_interval <= 0:
            return
        else:
            self.log.debug("Polling service status every %ims", poll_interval)

        self.stop_polling()

        self._poll_callback = PeriodicCallback(self.poll_and_notify, poll_interval)
        self._poll_callback.start()

    async def fetch(self, req, action):
        try:
            resp = await self.http_client.fetch(req)
        except HTTPClientError as e:
            if e.response:
                # Log failed response message for debugging purposes
                message = e.response.body.decode("utf8", "replace")
                try:
                    # guess json, reformat for readability
                    json_message = json.loads(message)
                except ValueError:
                    # not json
                    pass
                else:
                    # reformat json log message for readability
                    message = json.dumps(json_message, sort_keys=True, indent=1)
            else:
                # didn't get a response, e.g. connection error
                message = str(e)
            url = urlunparse(urlparse(req.url)._replace(query=""))
            self.log.error(
                f"Communication with backend failed: {e.code} {req.method} {url}: {message}.",
                extra={
                    "uuidcode": self.name,
                    "log_name": self._log_name,
                    "user": self.user.name,
                    "action": action,
                },
            )
            raise e
        else:
            if resp.body:
                return json.loads(resp.body.decode("utf8", "replace"))
            else:
                # empty body is None
                return None

    async def send_request(self, req, action, raise_exception=True):
        tic = time.monotonic()
        try:
            resp = await self.fetch(req, action)
        except Exception as tic_e:
            if raise_exception:
                raise tic_e
            else:
                return {}
        else:
            return resp
        finally:
            toc = str(time.monotonic() - tic)
            self.log.info(
                f"Communicated {action} with backend service ( {req.url} ) (request duration: {toc})",
                extra={
                    "uuidcode": self.name,
                    "log_name": self._log_name,
                    "user": self.user.name,
                    "duration": toc,
                },
            )

    async def start(self):
        self.log.info(f"Start single-user server {self.name}-{self.start_id}")
        request_body = await self.get_request_body_start()
        request_header = await self.get_request_headers_start()
        url = await self.get_request_url_start()

        req = HTTPRequest(
            url=url,
            method="POST",
            headers=request_header,
            body=json.dumps(request_body),
            **self.get_request_kwargs(),
        )

        try:
            resp_json = await self.send_request(req, action="start")
        except Exception as e:
            # If JupyterHub could not start the service, additional
            # actions may be required.
            await maybe_future(self.run_failed_spawn_request_hook(e))

            try:
                await self.stop()
            except:
                self.log.exception(
                    "Could not stop service which failed to start.",
                    extra={
                        "uuidcode": self.name,
                        "log_name": self._log_name,
                        "user": self.user.name,
                    },
                )
            # We already stopped everything we can stop at this stage.
            # With the raised exception JupyterHub will try to cancel again.
            # We can skip these stop attempts. Failed Spawners will be
            # available again faster.
            self.already_stopped = True
            self.already_post_stop_hooked = True

            raise e

        service_address = self.get_service_address()
        port = await self.get_port()
        self.log.debug(
            f"Expect JupyterLab at {service_address}:{port}",
            extra={
                "uuidcode": self.name,
                "log_name": self._log_name,
                "user": self.user.name,
            },
        )

        await maybe_future(self.run_post_spawn_request_hook(resp_json))
        return (service_address, port)

    async def poll(self):
        if self.already_stopped:
            # avoid loop with stop
            return 0

        url = await self.get_request_url_poll()
        headers = await self.get_request_headers_poll()
        req = HTTPRequest(
            url=url,
            method="GET",
            headers=headers,
            **self.get_request_kwargs(),
        )

        try:
            resp_json = await self.send_request(req, action="poll")
        except Exception as e:
            ret = 0
            if type(e).__name__ == "HTTPClientError" and getattr(e, "code", 500) == 404:
                if self.request_404_poll_keep_running:
                    ret = None
            if self.request_failed_poll_keep_running:
                ret = None
            return ret
        if not resp_json.get("running", True):
            return 0
        return None

    async def stop(self, now=False, cancel=False):
        if self.already_stopped:
            # We've already sent a request to the backend.
            # There's no need to do it again.
            return

        # Prevent multiple requests to the backend
        self.already_stopped = True

        url = await self.get_request_url_stop()
        headers = await self.get_request_headers_stop()
        req = HTTPRequest(
            url=url,
            method="DELETE",
            headers=headers,
            **self.get_request_kwargs(),
        )

        await self.send_request(req, action="stop", raise_exception=False)

        if self.cert_paths:
            Path(self.cert_paths["keyfile"]).unlink(missing_ok=True)
            Path(self.cert_paths["certfile"]).unlink(missing_ok=True)
            try:
                Path(self.cert_paths["certfile"]).parent.rmdir()
            except:
                pass

        # We've implemented a cancel feature, which allows you to call
        # Spawner.stop(cancel=True) and stop the spawn process.
        if cancel:
            await self.cancel()

    async def cancel(self):
        try:
            # If this function was called with cancel=True, it was called directly
            # and not via user.stop. So we want to cleanup the user object
            # as well. It will throw an exception, but we expect the asyncio task
            # to be cancelled, because we've cancelled it ourself.
            await self.user.stop(self.name)
        except asyncio.CancelledError:
            pass

        if type(self._spawn_future) is asyncio.Task:
            if self._spawn_future._state in ["PENDING"]:
                try:
                    self._spawn_future.cancel()
                    await maybe_future(self._spawn_future)
                except asyncio.CancelledError:
                    pass
