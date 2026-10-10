"""Call the sibling CLI spend policy. Do not copy it."""

import os
import sys
from pathlib import Path

_CLI_ROOT = Path(__file__).resolve().parents[2] / "cli"


def _load_cli_policy():
    cli_root = str(_CLI_ROOT)
    if cli_root not in sys.path:
        sys.path.insert(0, cli_root)
    # Puzzle modules and commands.config resolve paths from the CLI root at import.
    previous = os.getcwd()
    os.chdir(_CLI_ROOT)
    try:
        from commands.policy_runtime import (
            evaluate_chia_message_policy,
            evaluate_evm_message_policy,
            load_block_spends,
        )
        from commands.spend_policy import BridgeRoutes, PolicyResult
    finally:
        os.chdir(previous)
    return (
        evaluate_chia_message_policy,
        evaluate_evm_message_policy,
        load_block_spends,
        BridgeRoutes,
        PolicyResult,
    )


(
    evaluate_chia_message_policy,
    evaluate_evm_message_policy,
    load_block_spends,
    BridgeRoutes,
    PolicyResult,
) = _load_cli_policy()


def _is_evm(network) -> bool:
    network_type = getattr(network, "type", None)
    value = getattr(network_type, "value", network_type)
    return value == "evm"


def _erc20_parser(config, chain_id: str):
    for parser in config.parsers:
        if getattr(parser, "type", None) != "erc20bridge":
            continue
        if chain_id in parser.chains:
            return parser
    raise ValueError(f"no erc20bridge parser for {chain_id}")


def _millieth_address(parser) -> str:
    for field in ("millieth", "millieth_address", "millieth_contract"):
        value = getattr(parser, field, None)
        if value:
            return value
    for token in parser.tokens:
        if token.symbol == "milliETH":
            return token.contract
    raise ValueError("erc20bridge parser has no milliETH token or millieth contract field")


def build_bridge_routes(config) -> BridgeRoutes:
    """BridgeRoutes from watcher networks plus erc20bridge parser addresses."""
    xch = next((network for network in config.networks if network.id == "xch"), None)
    if xch is None or not xch.portal_launcher_id:
        raise ValueError("watcher config is missing xch.portal_launcher_id")

    cli_config = {"xch": {"portal_launcher_id": xch.portal_launcher_id}}
    for network in config.networks:
        if not _is_evm(network):
            continue
        if not network.portal_address:
            raise ValueError(f"{network.id}.portal_address is not set")
        if not network.portal_impl_codehash:
            raise ValueError(
                f"{network.id}.portal_impl_codehash is not set. "
                "Set it to the keccak256 of that chain's portal implementation runtime bytecode."
            )
        parser = _erc20_parser(config, network.id)
        cli_config[network.id] = {
            "portal_address": network.portal_address,
            "erc20_bridge_address": parser.contract,
            "millieth_address": _millieth_address(parser),
            "portal_impl_codehash": network.portal_impl_codehash,
        }
    return BridgeRoutes.from_config(cli_config)
