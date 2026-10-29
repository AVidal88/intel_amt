"""Data update coordinator for Intel AMT."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.debounce import Debouncer
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .amt_client import AmtClient, AmtDeviceInfo, AmtPowerStatus, AmtStatus
from .const import (
    CONF_HOST,
    CONF_PASSWORD,
    CONF_POWER_SCAN_INTERVAL,
    CONF_PROTOCOL,
    CONF_USERNAME,
    CONF_VERIFY_TLS,
    DEFAULT_SCAN_INTERVAL,
    DEFAULT_POWER_SCAN_INTERVAL,
    DEFAULT_VERIFY_TLS,
    DOMAIN,
    PLATFORMS,
)

_LOGGER = logging.getLogger(__name__)


class IntelAmtPowerCoordinator(DataUpdateCoordinator[AmtPowerStatus]):
    """Poll power independently of slower, optional AMT diagnostics."""

    config_entry: ConfigEntry
    device_info: AmtDeviceInfo | None = None

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, client: AmtClient) -> None:
        self.client = client
        self._operation_lock = asyncio.Lock()
        self._refresh_task: asyncio.Task[None] | None = None
        scan_interval = entry.options.get(
            CONF_POWER_SCAN_INTERVAL, DEFAULT_POWER_SCAN_INTERVAL
        )
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_power_{entry.data[CONF_HOST]}",
            update_interval=timedelta(seconds=scan_interval),
            always_update=False,
            request_refresh_debouncer=Debouncer(
                hass, _LOGGER, cooldown=1, immediate=True
            ),
        )
        self.config_entry = entry

    async def _async_update_data(self) -> AmtPowerStatus:
        try:
            async with self._operation_lock:
                return await self.hass.async_add_executor_job(
                    self.client.get_power_status
                )
        except Exception as err:
            raise UpdateFailed(f"AMT power poll failed: {err}") from err

    @callback
    def _async_refresh_after_command(self) -> None:
        """Refresh without holding the UI action open for the power transition."""
        if self._refresh_task is not None and not self._refresh_task.done():
            return
        self._refresh_task = self.config_entry.async_create_background_task(
            self.hass,
            self.async_request_refresh(),
            f"{DOMAIN}_power_refresh_{self.config_entry.entry_id}",
        )

    async def async_run_power(self, action: str) -> None:
        """Request a change; publish only the state later reported by AMT."""
        async with self._operation_lock:
            await self.hass.async_add_executor_job(self.client.set_power, action)
        self._async_refresh_after_command()

    async def async_pxe_boot(self) -> None:
        """Serialize boot configuration with power commands and fast polling."""
        async with self._operation_lock:
            await self.hass.async_add_executor_job(self.client.pxe_boot)
        self._async_refresh_after_command()


class IntelAmtCoordinator(DataUpdateCoordinator[AmtStatus]):
    """Poll diagnostics at their own interval and own the power coordinator."""

    config_entry: ConfigEntry
    device_info: AmtDeviceInfo | None = None

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, client: AmtClient) -> None:
        self.client = client
        scan_interval = entry.options.get("scan_interval", DEFAULT_SCAN_INTERVAL)
        super().__init__(
            hass,
            _LOGGER,
            name=f"{DOMAIN}_{entry.data[CONF_HOST]}",
            update_interval=timedelta(seconds=scan_interval),
        )
        self.config_entry = entry
        self.power_coordinator = IntelAmtPowerCoordinator(hass, entry, client)

    async def _async_update_data(self) -> AmtStatus:
        try:
            return await self.hass.async_add_executor_job(self.client.get_status)
        except Exception as err:
            raise UpdateFailed(f"AMT status poll failed: {err}") from err

    async def async_run_power(self, action: str) -> None:
        """Delegate power actions to the fast coordinator."""
        await self.power_coordinator.async_run_power(action)

    async def async_pxe_boot(self) -> None:
        """Delegate PXE boot to the fast coordinator."""
        await self.power_coordinator.async_pxe_boot()

    async def async_refresh_all(self) -> None:
        """Refresh both streams when the user explicitly requests an update."""
        await asyncio.gather(
            self.power_coordinator.async_request_refresh(),
            self.async_request_refresh(),
        )

    async def async_add_wake_alarm(
        self,
        start_time: datetime,
        instance_id: str,
        interval_minutes: int = 0,
        delete_on_completion: bool = True,
        element_name: str | None = None,
    ) -> None:
        """Schedule a firmware wake and refresh."""
        await self.hass.async_add_executor_job(
            self.client.add_wake_alarm,
            dt_util.as_utc(start_time),
            instance_id,
            interval_minutes,
            delete_on_completion,
            element_name,
        )
        await self.async_request_refresh()

    async def async_delete_wake_alarm(self, instance_id: str) -> None:
        """Delete a wake alarm and refresh."""
        await self.hass.async_add_executor_job(
            self.client.delete_wake_alarm, instance_id
        )
        await self.async_request_refresh()


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Intel AMT from a config entry."""
    client = AmtClient(
        host=entry.data[CONF_HOST],
        username=entry.data[CONF_USERNAME],
        password=entry.data[CONF_PASSWORD],
        protocol=entry.data[CONF_PROTOCOL],
        verify_tls=entry.options.get(
            CONF_VERIFY_TLS,
            entry.data.get(CONF_VERIFY_TLS, DEFAULT_VERIFY_TLS),
        ),
    )

    coordinator = IntelAmtCoordinator(hass, entry, client)

    try:
        await coordinator.async_config_entry_first_refresh()
        await coordinator.power_coordinator.async_config_entry_first_refresh()
    except ConfigEntryNotReady:
        raise
    except Exception as err:
        raise ConfigEntryNotReady(f"Unable to connect to AMT: {err}") from err

    try:
        coordinator.device_info = await hass.async_add_executor_job(
            client.get_device_info
        )
    except Exception as err:
        _LOGGER.warning("AMT device info fetch failed (continuing without): %s", err)

    coordinator.power_coordinator.device_info = coordinator.device_info
    hass.data.setdefault(DOMAIN, {})[entry.entry_id] = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload Intel AMT config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Reload config entry."""
    await async_unload_entry(hass, entry)
    await async_setup_entry(hass, entry)
