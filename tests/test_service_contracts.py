# Copyright (c) 2026 by C. Klukas.
# Licensed under the MIT License. See LICENSE for details.
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

import pytest

from paglets import (
    EmptyPayload,
    Host,
    Message,
    Paglet,
    PagletContext,
    PagletProxyRef,
    PagletState,
    ServiceContract,
    ServiceContractError,
    ServiceHandle,
    ServiceNotFoundError,
    ServiceOperation,
    ServiceRecord,
)
from tests.test_paglets_core import free_port


class Cabin(Enum):
    ECONOMY = "economy"
    BUSINESS = "business"


@dataclass(frozen=True, slots=True)
class Passenger:
    name: str


@dataclass(frozen=True, slots=True)
class QuoteRequest:
    origin: str
    destination: str
    passenger: Passenger
    cabin: Cabin


@dataclass(frozen=True, slots=True)
class QuoteReply:
    price: float
    currency: str


@dataclass(frozen=True, slots=True)
class WatchRequest:
    route: str


@dataclass(frozen=True, slots=True)
class WatchReply:
    accepted: bool


QUOTE = ServiceOperation("quote", QuoteRequest, QuoteReply)
PING = ServiceOperation("ping", EmptyPayload, EmptyPayload)
WATCH = ServiceOperation("watch", WatchRequest, WatchReply)
FLIGHT_TICKETS = ServiceContract("flight-ticket", operations=(QUOTE, PING), version="1")
FLIGHT_TICKETS_V2 = ServiceContract("flight-ticket", operations=(QUOTE, PING), version="2")


@dataclass
class TicketState(PagletState):
    advertised: bool = False


class TicketAgent(Paglet[TicketState]):
    State = TicketState

    def on_creation(self, event):
        self.advertise_contract(FLIGHT_TICKETS, scope="mesh", metadata={"provider": "test"})
        self.state.advertised = True

    def quote(self, request: QuoteRequest) -> QuoteReply:
        multiplier = 2.0 if request.cabin is Cabin.BUSINESS else 1.0
        return QuoteReply(price=199.0 * multiplier, currency="EUR")

    def ping(self, request: EmptyPayload) -> EmptyPayload:
        return EmptyPayload()

    def handle_message(self, message: Message):
        return FLIGHT_TICKETS.route(
            message,
            {QUOTE: self.quote, PING: self.ping},
            default=self.not_handled(),
        )


@dataclass
class ClientState(PagletState):
    last_price: float = 0.0
    last_currency: str = ""


class ClientAgent(Paglet[ClientState]):
    State = ClientState

    def handle_message(self, message: Message):
        if message.kind == "quote":
            tickets = self.require_contract(FLIGHT_TICKETS, operation=QUOTE, scope="mesh")
            reply = tickets.call(
                QUOTE,
                QuoteRequest(
                    origin="FRA",
                    destination="SFO",
                    passenger=Passenger("Ada"),
                    cabin=Cabin.BUSINESS,
                ),
            )
            self.state.last_price = reply.price
            self.state.last_currency = reply.currency
            return {"price": reply.price, "currency": reply.currency}
        if message.kind == "ping-service":
            tickets = self.require_contract(FLIGHT_TICKETS, operation=PING, scope="mesh")
            return tickets.call(PING) == EmptyPayload()
        return self.not_handled()


class LegacyServiceAgent(Paglet[TicketState]):
    State = TicketState

    def on_creation(self, event):
        self.advertise_service("quotes", capabilities=("quote",), scope="mesh")


class BrokenTicketAgent(Paglet[TicketState]):
    State = TicketState

    def on_creation(self, event):
        self.advertise_contract(FLIGHT_TICKETS)

    def handle_message(self, message: Message):
        return FLIGHT_TICKETS.route(
            message,
            {QUOTE: lambda request: "not-a-quote-reply"},
            default=self.not_handled(),
        )


def test_invalid_contracts_reject_bad_definitions():
    with pytest.raises(ServiceContractError):
        ServiceContract("", operations=(QUOTE,))
    with pytest.raises(ServiceContractError):
        ServiceContract("empty", operations=())
    with pytest.raises(ServiceContractError):
        ServiceContract("duplicate", operations=(QUOTE, ServiceOperation("quote", QuoteRequest, QuoteReply)))
    with pytest.raises(ServiceContractError):
        ServiceOperation("bad-request", dict, QuoteReply)

    @dataclass(frozen=True, slots=True)
    class LocalRequest:
        value: str

    with pytest.raises(ServiceContractError):
        ServiceOperation("local", LocalRequest, EmptyPayload)
    with pytest.raises(ServiceContractError):
        FLIGHT_TICKETS.advertise_metadata({"paglets.service_contract": {"name": "user-owned"}})


def test_operation_serialization_round_trips_nested_enums_and_empty_payload():
    request = QuoteRequest("FRA", "SFO", Passenger("Ada"), Cabin.BUSINESS)
    message = QUOTE.to_message(request)

    assert message.kind == "quote"
    assert message.args == {
        "origin": "FRA",
        "destination": "SFO",
        "passenger": {"name": "Ada"},
        "cabin": "business",
    }
    assert QUOTE.decode_request(message) == request

    reply = QuoteReply(398.0, "EUR")
    assert QUOTE.decode_reply(QUOTE.encode_reply(reply)) == reply

    empty_message = PING.to_message()
    assert empty_message.args == {}
    assert PING.decode_request(empty_message) == EmptyPayload()
    assert PING.decode_reply({}) == EmptyPayload()
    assert PING.decode_reply(None) == EmptyPayload()


def test_typed_contract_discovers_and_calls_mesh_service(tmp_path):
    alpha = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh_version="typed-services-test",
        persistence_dir=tmp_path / "alpha",
    )
    beta = Host(
        "beta",
        host="127.0.0.1",
        port=free_port(),
        peers=[alpha.address],
        mesh_version="typed-services-test",
        persistence_dir=tmp_path / "beta",
    )
    alpha.start_background()
    beta.start_background()
    try:
        beta.mesh.gossip_once()
        alpha.mesh.gossip_once()
        beta.create(TicketAgent, TicketState())
        client = alpha.create(ClientAgent, ClientState())

        assert client.send(Message("quote")) == {"price": 398.0, "currency": "EUR"}
        assert client.send(Message("ping-service")) is True

        context = PagletContext(alpha)
        handle = context.lookup_contract(FLIGHT_TICKETS, operation=QUOTE, scope="mesh")
        assert handle is not None
        assert handle.record.metadata["provider"] == "test"
        assert context.lookup_contract(FLIGHT_TICKETS_V2, operation=QUOTE, scope="mesh") is None
        with pytest.raises(ServiceNotFoundError):
            context.require_contract(FLIGHT_TICKETS_V2, operation=QUOTE, scope="mesh")
    finally:
        beta.stop()
        alpha.stop()


def test_legacy_string_services_remain_discoverable_but_not_typed(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        proxy = host.create(LegacyServiceAgent, TicketState())

        legacy = host.lookup_service("quotes", capability="quote")
        assert legacy is not None
        assert legacy.proxy.agent_id == proxy.agent_id
        assert PagletContext(host).lookup_contract(FLIGHT_TICKETS) is None
    finally:
        host.stop()


def test_contract_failure_modes_are_reported_before_untyped_calls():
    record = ServiceRecord(
        name=FLIGHT_TICKETS.name,
        proxy=PagletProxyRef("http://127.0.0.1:9", "agent"),
        capabilities=FLIGHT_TICKETS.capabilities,
        metadata=FLIGHT_TICKETS.advertise_metadata(),
    )
    handle = ServiceHandle(FLIGHT_TICKETS, record)

    with pytest.raises(ServiceContractError):
        FLIGHT_TICKETS.require_operation(WATCH)
    with pytest.raises(ServiceContractError):
        handle.call(WATCH, WatchRequest("FRA-SFO"))
    with pytest.raises(ServiceContractError):
        QUOTE.to_message(EmptyPayload())
    with pytest.raises(ServiceContractError):
        FLIGHT_TICKETS.route(
            Message("quote", {"origin": "FRA"}),
            {QUOTE: lambda request: QuoteReply(1.0, "EUR")},
            default=None,
        )
    with pytest.raises(ServiceContractError):
        FLIGHT_TICKETS.route(
            QUOTE.to_message(QuoteRequest("FRA", "SFO", Passenger("Ada"), Cabin.ECONOMY)),
            {QUOTE: lambda request: "not-a-reply"},
            default=None,
        )

    assert FLIGHT_TICKETS.route(Message("unknown"), {}, default="fallback") == "fallback"


def test_remote_contract_errors_round_trip_as_contract_errors(tmp_path):
    host = Host(
        "alpha",
        host="127.0.0.1",
        port=free_port(),
        mesh=False,
        mesh_multicast=False,
        persistence_dir=tmp_path / "alpha",
    )
    host.start_background()
    try:
        host.create(BrokenTicketAgent, TicketState())
        handle = PagletContext(host).require_contract(FLIGHT_TICKETS, operation=QUOTE)

        with pytest.raises(ServiceContractError):
            handle.call(QUOTE, QuoteRequest("FRA", "SFO", Passenger("Ada"), Cabin.ECONOMY))
    finally:
        host.stop()
