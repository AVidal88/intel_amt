"""Exercise automatic power updates with real coordinators, entities, and timers."""

import asyncio
from collections.abc import Callable, Generator
from datetime import timedelta
from threading import Event as ThreadEvent
from unittest.mock import MagicMock, Mock, patch

from freezegun.api import FrozenDateTimeFactory
import pytest
from requests import Response
from requests.exceptions import Timeout

from homeassistant.const import STATE_OFF, STATE_ON, STATE_UNAVAILABLE, STATE_UNKNOWN
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_time_changed,
)

from custom_components.intel_amt.amt_client import (
    CIM_ASSOCIATED_POWER,
    AmtClient,
    AmtPowerStatus,
)
from custom_components.intel_amt.const import DOMAIN
from custom_components.intel_amt.sensor import (
    STATUS_SENSORS,
    IntelAmtPowerSensor,
    IntelAmtStatusSensor,
)

DEVICE_DATA = {
    "host": "192.0.2.10",
    "username": "admin",
    "password": "test-password",
    "protocol": "https",
    "name": "Workstation",
    "verify_tls": False,
}


@pytest.fixture
def power_transport(mock_amt_post: MagicMock) -> dict:
    """Control the simulated device independently of Home Assistant commands."""
    device = {"code": 2, "error": None}
    default_response = mock_amt_post.side_effect

    def respond(url: str, *, data: str, **kwargs: object) -> Response:
        response = default_response(url, data=data, **kwargs)
        if CIM_ASSOCIATED_POWER not in data:
            return response
        if device["error"] is not None:
            raise device["error"]
        code = device["code"]
        state = "" if code is None else f"<PowerState>{code}</PowerState>"
        response._content = (
            '<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope">'
            f"<s:Body><Response>{state}"
            "<AvailableRequestedPowerStates>2 8</AvailableRequestedPowerStates>"
            "</Response></s:Body></s:Envelope>"
        ).encode()
        return response

    mock_amt_post.side_effect = respond
    return device


@pytest.fixture
def power_platforms() -> Generator[None]:
    """Load real power entities and one diagnostic entity, without Recorder."""

    async def setup_sensors(hass, entry, async_add_entities) -> None:
        coordinator = hass.data[DOMAIN][entry.entry_id]
        async_add_entities(
            [
                IntelAmtPowerSensor(coordinator),
                IntelAmtStatusSensor(coordinator, STATUS_SENSORS[0]),
            ]
        )

    with (
        patch("custom_components.intel_amt.coordinator.PLATFORMS", ["sensor", "switch"]),
        patch(
            "custom_components.intel_amt.sensor.async_setup_entry",
            side_effect=setup_sensors,
        ),
    ):
        yield


@pytest.fixture
def setup_power_entry(
    hass: HomeAssistant, power_transport: dict, power_platforms: None
) -> Callable:
    """Set up a real config entry with both polling coordinators subscribed."""

    async def setup(options: dict | None = None):
        entry = MockConfigEntry(
            domain=DOMAIN,
            title="Workstation",
            unique_id=DEVICE_DATA["host"],
            data=DEVICE_DATA,
            options=options or {},
        )
        entry.add_to_hass(hass)
        assert await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()
        return entry, hass.data[DOMAIN][entry.entry_id]

    return setup


def entity_state(hass: HomeAssistant, entry: MockConfigEntry, platform: str) -> str:
    """Read the actual entity state using its persistent registry identity."""
    entity_id = er.async_get(hass).async_get_entity_id(
        platform, DOMAIN, f"{entry.entry_id}_power"
    )
    assert entity_id is not None
    state = hass.states.get(entity_id)
    assert state is not None
    return state.state


async def advance(
    hass: HomeAssistant, freezer: FrozenDateTimeFactory, seconds: int = 5
) -> None:
    """Run due Home Assistant timers without sleeping in real time."""
    freezer.tick(timedelta(seconds=seconds))
    async_fire_time_changed(hass)
    await hass.async_block_till_done(wait_background_tasks=True)


def test_power_status_uses_one_request(
    power_transport: dict, mock_amt_post: MagicMock
) -> None:
    """The fast path reads power without enumerating diagnostic features."""
    client = AmtClient(
        host="192.0.2.10", username="admin", password="test-password", protocol="https"
    )
    result = client.get_power_status()
    assert isinstance(result, AmtPowerStatus)
    assert result.power_state == "on"
    assert result.available_transitions == ["on", "off"]
    mock_amt_post.assert_called_once()
    assert CIM_ASSOCIATED_POWER in mock_amt_post.call_args.kwargs["data"]


async def test_external_changes_and_independent_poll_intervals(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    setup_power_entry: Callable,
    power_transport: dict,
    mock_amt_post: MagicMock,
) -> None:
    """External changes reach entities in 5s while diagnostics still run at 120s."""
    entry, coordinator = await setup_power_entry()
    assert entity_state(hass, entry, "switch") == STATE_ON
    changes = Mock()
    remove_listener = coordinator.power_coordinator.async_add_listener(changes)
    mock_amt_post.reset_mock()

    try:
        with patch.object(
            coordinator.client, "get_status", wraps=coordinator.client.get_status
        ) as diagnostics:
            power_transport["code"] = 8
            await advance(hass, freezer)
            assert entity_state(hass, entry, "switch") == STATE_OFF
            assert entity_state(hass, entry, "sensor") == "off"
            assert diagnostics.call_count == 0
            assert changes.call_count == 1

            await advance(hass, freezer)
            assert changes.call_count == 1  # Unchanged data does not notify listeners.

            power_transport["code"] = 2
            await advance(hass, freezer)
            assert entity_state(hass, entry, "switch") == STATE_ON
            assert entity_state(hass, entry, "sensor") == "on"
            assert changes.call_count == 2

            for _ in range(21):
                await advance(hass, freezer)
            assert diagnostics.call_count == 1

        power_reads = [
            call
            for call in mock_amt_post.call_args_list
            if CIM_ASSOCIATED_POWER in call.kwargs["data"]
        ]
        # 24 lightweight polls plus the power read inside one full status refresh.
        assert len(power_reads) == 25
        assert not any(
            "RequestPowerStateChange_INPUT" in call.kwargs["data"]
            for call in mock_amt_post.call_args_list
        )
    finally:
        remove_listener()


async def test_slow_diagnostics_do_not_block_power(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    setup_power_entry: Callable,
    power_transport: dict,
) -> None:
    """A diagnostic request held in its executor does not stall fast power updates."""
    entry, coordinator = await setup_power_entry()
    started = asyncio.Event()
    release = ThreadEvent()
    changed = asyncio.Event()
    original = coordinator.client.get_status

    def slow_status():
        hass.loop.call_soon_threadsafe(started.set)
        assert release.wait(5), "Test did not release the diagnostic request"
        return original()

    def power_changed() -> None:
        if coordinator.power_coordinator.data.power_state == "off":
            changed.set()

    remove_listener = coordinator.power_coordinator.async_add_listener(power_changed)
    # Use a function, not Mock: the HA test fixture runs Mock jobs inline.
    with patch.object(coordinator.client, "get_status", new=slow_status):
        task = entry.async_create_background_task(
            hass, coordinator.async_refresh(), "test slow diagnostics"
        )
        try:
            await started.wait()
            power_transport["code"] = 8
            freezer.tick(timedelta(seconds=5))
            async_fire_time_changed(hass)
            await changed.wait()
            assert not task.done()
            assert entity_state(hass, entry, "switch") == STATE_OFF
        finally:
            release.set()
            await task
            remove_listener()
    await hass.async_block_till_done()


async def test_timeout_is_unavailable_and_recovers(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    setup_power_entry: Callable,
    power_transport: dict,
) -> None:
    """A missing AMT response is not reported as a powered-off workstation."""
    entry, _ = await setup_power_entry()
    power_transport["error"] = Timeout("AMT did not respond")
    await advance(hass, freezer)
    assert entity_state(hass, entry, "switch") == STATE_UNAVAILABLE
    assert entity_state(hass, entry, "sensor") == STATE_UNAVAILABLE

    power_transport.update(error=None, code=8)
    await advance(hass, freezer)
    assert entity_state(hass, entry, "switch") == STATE_OFF
    assert entity_state(hass, entry, "sensor") == "off"


@pytest.mark.parametrize("code", [None, 5, 999], ids=["missing", "rebooting", "unknown"])
async def test_unknown_or_transitional_power_is_not_off(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    setup_power_entry: Callable,
    power_transport: dict,
    code: int | None,
) -> None:
    """Missing, unfamiliar, and transitional states must not invent an off state."""
    entry, _ = await setup_power_entry()
    power_transport["code"] = code
    await advance(hass, freezer)
    assert entity_state(hass, entry, "switch") in (STATE_UNKNOWN, STATE_UNAVAILABLE)


async def test_command_returns_before_status_refresh_finishes(
    hass: HomeAssistant, setup_power_entry: Callable, power_transport: dict
) -> None:
    """A command waits for its acknowledgement, not for the computer to boot."""
    power_transport["code"] = 8
    entry, coordinator = await setup_power_entry()
    started = asyncio.Event()
    release = asyncio.Event()

    async def delayed_refresh() -> AmtPowerStatus:
        started.set()
        await release.wait()
        return AmtPowerStatus(power_state="on", available_transitions=[])

    with patch.object(
        coordinator.power_coordinator, "_async_update_data", side_effect=delayed_refresh
    ):
        command = hass.async_create_task(coordinator.async_run_power("on"))
        try:
            await started.wait()
            await asyncio.sleep(0)
            assert command.done()
            assert command.exception() is None
            assert entity_state(hass, entry, "switch") == STATE_OFF
        finally:
            release.set()
            await command
            await hass.async_block_till_done(wait_background_tasks=True)
    assert entity_state(hass, entry, "switch") == STATE_ON


async def test_unload_cancels_pending_refresh_and_polling(
    hass: HomeAssistant,
    freezer: FrozenDateTimeFactory,
    setup_power_entry: Callable,
    mock_amt_post: MagicMock,
) -> None:
    """Unloading stops both schedules and the post-command background task."""
    entry, coordinator = await setup_power_entry()
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def pending_refresh() -> AmtPowerStatus:
        started.set()
        try:
            await release.wait()
            return AmtPowerStatus(power_state="on", available_transitions=[])
        finally:
            finished.set()

    with patch.object(
        coordinator.power_coordinator, "_async_update_data", side_effect=pending_refresh
    ):
        command = hass.async_create_task(coordinator.async_run_power("on"))
        try:
            await started.wait()
            assert await hass.config_entries.async_unload(entry.entry_id)
            assert finished.is_set()
        finally:
            release.set()
            await command
            await hass.async_block_till_done(wait_background_tasks=True)

    calls_after_unload = mock_amt_post.call_count
    await advance(hass, freezer, 130)
    assert mock_amt_post.call_count == calls_after_unload


async def test_poll_and_command_are_serialized(
    hass: HomeAssistant, setup_power_entry: Callable, mock_amt_post: MagicMock
) -> None:
    """A command cannot race with a power request already in progress."""
    _, coordinator = await setup_power_entry()
    started = asyncio.Event()
    release = ThreadEvent()
    original = coordinator.client.get_power_status

    def delayed_power():
        hass.loop.call_soon_threadsafe(started.set)
        assert release.wait(5), "Test did not release the power request"
        return original()

    with patch.object(coordinator.client, "get_power_status", new=delayed_power):
        query = hass.async_create_task(coordinator.power_coordinator.async_refresh())
        await started.wait()
        mock_amt_post.reset_mock()
        command = hass.async_create_task(coordinator.async_run_power("on"))
        try:
            await asyncio.sleep(0)
            assert not command.done()
            assert mock_amt_post.call_count == 0
        finally:
            release.set()
            await asyncio.gather(query, command)
            await hass.async_block_till_done(wait_background_tasks=True)
    assert any(
        "RequestPowerStateChange_INPUT" in call.kwargs["data"]
        for call in mock_amt_post.call_args_list
    )


async def test_poll_options_preserve_tls_and_reload_both_intervals(
    hass: HomeAssistant, setup_power_entry: Callable
) -> None:
    """An older entry keeps its diagnostic setting and gains an independent fast rate."""
    entry, _ = await setup_power_entry({"scan_interval": 240, "verify_tls": False})
    result = await hass.config_entries.options.async_init(entry.entry_id)
    result = await hass.config_entries.options.async_configure(
        result["flow_id"], user_input={"next_step_id": "scan_interval"}
    )
    defaults = result["data_schema"]({})
    assert defaults["scan_interval"] == 240
    assert defaults["power_scan_interval"] == 5
    result = await hass.config_entries.options.async_configure(
        result["flow_id"],
        user_input={"scan_interval": 180, "power_scan_interval": 3},
    )
    await hass.async_block_till_done()
    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert entry.options == {
        "scan_interval": 180,
        "power_scan_interval": 3,
        "verify_tls": False,
    }
    coordinator = hass.data[DOMAIN][entry.entry_id]
    assert coordinator.update_interval == timedelta(seconds=180)
    assert coordinator.power_coordinator.update_interval == timedelta(seconds=3)
    assert coordinator.client.verify_tls is False
