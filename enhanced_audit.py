#!/usr/bin/env python3
"""
Enhanced Audit System with Rule of Three
Per universe_04.txt requirements for robust statistical auditing.

The "rule of three" states that if zero events are observed in a sample of size n,
the upper 95% confidence bound on the event rate is approximately 3/n.

To guarantee miss rate < 1% with 95% confidence: n >= 300
To guarantee miss rate < 0.5% with 95% confidence: n >= 600
"""
import math
import random
from typing import List, Dict, Any, Optional
from run_zero_miss import (
    log, get_premarket_high_theta_v3, calc_r1_premarket_gap_pct, ThetaV3
)

def calculate_required_sample_size(target_miss_rate: float = 0.01, confidence: float = 0.95) -> int:
    """
    Calculate required sample size using rule of three for zero-event sampling.

    For 0 events observed in sample size n:
    Upper bound on true rate (95% confidence) ~ 3/n

    To ensure bound <= target_miss_rate: 3/n <= target_miss_rate
    Therefore: n >= 3/target_miss_rate

    Args:
        target_miss_rate: Maximum acceptable miss rate (default 1% = 0.01)
        confidence: Confidence level (default 95% = 0.95)

    Returns:
        Required sample size
    """
    if target_miss_rate <= 0:
        raise ValueError("Target miss rate must be positive")

    # Rule of three constant (3 for 95% confidence)
    rule_constant = 3.0
    if confidence == 0.90:
        rule_constant = 2.3  # 90% confidence
    elif confidence == 0.99:
        rule_constant = 4.6  # 99% confidence

    required_n = math.ceil(rule_constant / target_miss_rate)

    log(f"Rule of three calculation:")
    log(f"  Target miss rate: {target_miss_rate:.2%}")
    log(f"  Confidence level: {confidence:.1%}")
    log(f"  Required sample size: {required_n}")

    return required_n

def calculate_miss_rate_bound(sample_size: int, observed_misses: int = 0, confidence: float = 0.95) -> float:
    """
    Calculate upper confidence bound on miss rate given sample results.

    For observed_misses = 0: uses rule of three (3/n)
    For observed_misses > 0: uses exact binomial confidence interval

    Args:
        sample_size: Size of audit sample
        observed_misses: Number of missed events found
        confidence: Confidence level

    Returns:
        Upper bound on true miss rate
    """
    if sample_size <= 0:
        return 1.0

    if observed_misses == 0:
        # Rule of three for zero events
        rule_constant = 3.0 if confidence == 0.95 else 2.3 if confidence == 0.90 else 4.6
        bound = rule_constant / sample_size
    else:
        # Exact binomial confidence interval (using Wilson score interval)
        # For simplicity, use conservative estimate
        p_hat = observed_misses / sample_size
        z = 1.96 if confidence == 0.95 else 1.645 if confidence == 0.90 else 2.576

        # Wilson score upper bound
        numerator = p_hat + (z**2)/(2*sample_size) + z * math.sqrt((p_hat*(1-p_hat) + z**2/(4*sample_size))/sample_size)
        denominator = 1 + z**2/sample_size
        bound = numerator / denominator

    return min(bound, 1.0)

def conduct_enhanced_miss_audit(date_ymd: str, exchange_roster: List[str],
                              discovered_r1_hits: List[Dict[str, Any]],
                              theta_v3: Optional[ThetaV3], prev_close_map: Dict[str, float],
                              target_miss_rate: float = 0.01,
                              confidence: float = 0.95,
                              max_sample_size: int = 1000) -> Dict[str, Any]:
    """
    Enhanced miss audit using rule of three for sample sizing.

    Args:
        date_ymd: Date to audit
        exchange_roster: All symbols traded on the date
        discovered_r1_hits: R1 hits found by primary discovery
        theta_v3: ThetaData v3 client
        prev_close_map: Previous close prices
        target_miss_rate: Target maximum miss rate (default 1%)
        confidence: Statistical confidence level (default 95%)
        max_sample_size: Maximum sample size to prevent excessive API calls

    Returns:
        Enhanced audit results with statistical bounds
    """
    if not theta_v3:
        log("Enhanced audit requires ThetaData v3")
        return {"audit_passed": False, "reason": "no_theta_v3"}

    log(f"Enhanced miss audit for {date_ymd}")
    log(f"Exchange roster: {len(exchange_roster)} symbols")
    log(f"Discovered R1 hits: {len(discovered_r1_hits)}")

    # Get symbols we already discovered as R1 hits
    discovered_symbols = {hit["symbol"] for hit in discovered_r1_hits}

    # Get symbols not in our discovery (potential misses)
    undiscovered_symbols = [s for s in exchange_roster if s not in discovered_symbols]

    log(f"Undiscovered symbols to audit: {len(undiscovered_symbols)}")

    if len(undiscovered_symbols) == 0:
        log("No undiscovered symbols to audit - complete coverage")
        return {
            "audit_passed": True,
            "sample_size": 0,
            "samples_checked": 0,
            "missed_r1_hits": [],
            "miss_rate_bound": 0.0,
            "target_miss_rate": target_miss_rate,
            "confidence_level": confidence,
            "reason": "complete_coverage"
        }

    # Calculate required sample size using rule of three
    required_sample_size = calculate_required_sample_size(target_miss_rate, confidence)

    # Use the smaller of required size, available symbols, and max limit
    actual_sample_size = min(required_sample_size, len(undiscovered_symbols), max_sample_size)

    log(f"Sample sizing:")
    log(f"  Required (rule of three): {required_sample_size}")
    log(f"  Available symbols: {len(undiscovered_symbols)}")
    log(f"  Max allowed: {max_sample_size}")
    log(f"  Actual sample size: {actual_sample_size}")

    # Random sampling with fixed seed for reproducibility
    random.seed(42)
    sample_symbols = random.sample(undiscovered_symbols, actual_sample_size)

    # Conduct audit
    missed_r1_hits = []
    samples_checked = 0
    audit_errors = 0

    for i, symbol in enumerate(sample_symbols, 1):
        try:
            # Get premarket high for this symbol
            pm_high = get_premarket_high_theta_v3(symbol, date_ymd, theta_v3)

            if pm_high and pm_high > 0:
                # Get prev_close
                prev_close = prev_close_map.get(symbol)

                if prev_close and prev_close > 0:
                    # Calculate R1 premarket gap
                    r1_pct = calc_r1_premarket_gap_pct(pm_high, prev_close)

                    if r1_pct and r1_pct >= 50.0:
                        # This is a MISS - we should have discovered this!
                        missed_r1_hits.append({
                            "symbol": symbol,
                            "rule": "R1",
                            "value": r1_pct,
                            "premarket_high": pm_high,
                            "prev_close": prev_close,
                            "date": date_ymd,
                            "audit_discovered": True
                        })
                        log(f"MISS DETECTED: {symbol} had {r1_pct:.1f}% R1 gap")

            samples_checked += 1

            # Progress logging
            if i % 50 == 0:
                log(f"Audit progress: {i}/{actual_sample_size} samples")

        except Exception as e:
            audit_errors += 1
            log(f"Audit error for {symbol}: {e}")
            continue

    # Calculate statistical bounds
    observed_misses = len(missed_r1_hits)
    miss_rate_bound = calculate_miss_rate_bound(samples_checked, observed_misses, confidence)

    # Determine audit result
    audit_passed = (observed_misses == 0 and miss_rate_bound <= target_miss_rate)

    log(f"Enhanced audit results:")
    log(f"  Samples checked: {samples_checked}")
    log(f"  Audit errors: {audit_errors}")
    log(f"  Observed misses: {observed_misses}")
    log(f"  Miss rate bound: {miss_rate_bound:.2%} (target: {target_miss_rate:.2%})")
    log(f"  Confidence level: {confidence:.1%}")
    log(f"  Audit result: {'PASS' if audit_passed else 'FAIL'}")

    if not audit_passed:
        if observed_misses > 0:
            log("AUDIT FAILURE - Discovery system missed R1 events!")
            for miss in missed_r1_hits:
                log(f"MISSED: {miss['symbol']} {miss['value']:.1f}%")
        else:
            log("AUDIT FAILURE - Sample size insufficient for target miss rate bound")

    return {
        "audit_passed": audit_passed,
        "sample_size": actual_sample_size,
        "samples_checked": samples_checked,
        "missed_r1_hits": missed_r1_hits,
        "miss_rate_bound": miss_rate_bound,
        "target_miss_rate": target_miss_rate,
        "confidence_level": confidence,
        "required_sample_size": required_sample_size,
        "exchange_roster_size": len(exchange_roster),
        "undiscovered_count": len(undiscovered_symbols),
        "audit_errors": audit_errors,
        "reason": "statistical_audit_complete"
    }

def build_enhanced_miss_audit_system(date_ymd: str, r1_hits: List[Dict[str, Any]],
                                   theta_v3: Optional[ThetaV3], prev_close_map: Dict[str, float],
                                   api_key: str, target_miss_rate: float = 0.01) -> Dict[str, Any]:
    """
    Enhanced miss audit system with rule of three implementation.
    Wrapper function compatible with existing validation framework.

    Args:
        date_ymd: Date to audit
        r1_hits: R1 hits found by primary discovery
        theta_v3: ThetaData v3 client
        prev_close_map: Previous close prices
        api_key: Polygon API key
        target_miss_rate: Target maximum miss rate (default 1%)

    Returns:
        Enhanced audit results
    """
    from run_zero_miss import get_exchange_active_roster_polygon

    log("=" * 80)
    log("ENHANCED MISS AUDIT SYSTEM (Rule of Three)")
    log("=" * 80)

    # Step 1: Get exchange-active roster from Polygon
    exchange_roster = get_exchange_active_roster_polygon(date_ymd, api_key)

    if not exchange_roster:
        log("Failed to get exchange roster from Polygon - audit cannot proceed")
        return {
            "audit_passed": False,
            "reason": "no_exchange_roster",
            "exchange_roster_size": 0
        }

    # Step 2: Conduct enhanced audit with rule of three
    audit_results = conduct_enhanced_miss_audit(
        date_ymd, exchange_roster, r1_hits, theta_v3, prev_close_map, target_miss_rate
    )

    log("Enhanced miss audit system completed")
    return audit_results