"""
Normalize game prize fields across DB variants (ETB columns vs legacy USD-only).
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional


def normalize_game_prize_fields(g: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Ensure prize_pool_etb and prize_pool_remaining exist for API / UI (mutates g).
    Older schemas only have prize_pool_usd (and maybe max_payout_usd).
    """
    if not g:
        return g
    etb = g.get("prize_pool_etb")
    usd = g.get("prize_pool_usd")
    if etb is None and usd is not None:
        try:
            g["prize_pool_etb"] = float(usd)
        except (TypeError, ValueError):
            pass
    if g.get("prize_pool_remaining") is None and g.get("prize_pool_etb") is not None:
        try:
            g["prize_pool_remaining"] = float(g["prize_pool_etb"])
        except (TypeError, ValueError):
            pass
    return g


def normalize_games_list(games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for x in games:
        normalize_game_prize_fields(x)
    return games
