"""Authenticated external-channel adapters."""

from .base import ChannelContact, ChannelMessage, ChannelReceipt
from .service import ChannelService, ExternalContactRepository

__all__ = [
    "ChannelContact",
    "ChannelMessage",
    "ChannelReceipt",
    "ChannelService",
    "ExternalContactRepository",
]
