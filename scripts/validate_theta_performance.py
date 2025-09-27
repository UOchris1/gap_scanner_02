# -*- coding: ascii -*-
# ThetaData concurrency and performance validation for Standard tier

import sys
import os
import time
import threading
import json
import argparse
from datetime import datetime
from typing import Dict, List, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.providers.theta_provider import ThetaDataClient

def test_basic_connectivity() -> Dict[str, Any]:
    """Test basic ThetaData connectivity and version detection"""
    print("[CONNECTIVITY] Testing ThetaData connectivity...")

    theta = ThetaDataClient(timeout_sec=10)

    result = {
        "timestamp": datetime.now().isoformat(),
        "theta_ok": theta.ok(),
        "version": theta.version,
        "base_url": theta.base,
        "param_style": theta.param
    }

    if theta.ok():
        print(f"[CONNECTIVITY] SUCCESS - {theta.version} on {theta.base} (param: {theta.param})")
    else:
        print(f"[CONNECTIVITY] FAILED - No ThetaData connection available")

    return result

def test_single_request_performance(theta: ThetaDataClient, symbol: str, test_date: str) -> Dict[str, Any]:
    """Test performance of a single request"""
    start_time = time.time()

    try:
        # Test EOD request
        eod_start = time.time()
        prev_close = theta.get_prev_close_eod(symbol, test_date)
        eod_duration = time.time() - eod_start

        # Test premarket request
        pm_start = time.time()
        pm_high = theta.get_premarket_high(symbol, test_date)
        pm_duration = time.time() - pm_start

        total_duration = time.time() - start_time

        return {
            "symbol": symbol,
            "test_date": test_date,
            "success": True,
            "prev_close": prev_close,
            "pm_high": pm_high,
            "eod_duration_ms": round(eod_duration * 1000, 2),
            "pm_duration_ms": round(pm_duration * 1000, 2),
            "total_duration_ms": round(total_duration * 1000, 2)
        }

    except Exception as e:
        total_duration = time.time() - start_time
        return {
            "symbol": symbol,
            "test_date": test_date,
            "success": False,
            "error": str(e),
            "total_duration_ms": round(total_duration * 1000, 2)
        }

def test_concurrent_requests(symbols: List[str], test_date: str, max_workers: int = 4) -> Dict[str, Any]:
    """Test concurrent request handling with semaphore limits"""
    print(f"[CONCURRENCY] Testing {len(symbols)} symbols with {max_workers} concurrent workers...")

    theta = ThetaDataClient()

    if not theta.ok():
        return {
            "success": False,
            "error": "ThetaData not available"
        }

    start_time = time.time()
    results = []
    errors = []

    # Track active requests
    active_requests = []
    request_times = []

    def make_request(symbol: str) -> Dict[str, Any]:
        request_start = time.time()
        active_requests.append({"symbol": symbol, "start_time": request_start})

        try:
            result = test_single_request_performance(theta, symbol, test_date)
            request_end = time.time()
            request_duration = request_end - request_start

            request_times.append(request_duration)

            # Remove from active requests
            active_requests[:] = [r for r in active_requests if r["symbol"] != symbol]

            return result

        except Exception as e:
            request_end = time.time()
            request_duration = request_end - request_start
            request_times.append(request_duration)

            # Remove from active requests
            active_requests[:] = [r for r in active_requests if r["symbol"] != symbol]

            return {
                "symbol": symbol,
                "success": False,
                "error": str(e),
                "duration_ms": round(request_duration * 1000, 2)
            }

    # Execute concurrent requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all requests
        future_to_symbol = {executor.submit(make_request, symbol): symbol for symbol in symbols}

        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                result = future.result()
                results.append(result)

                if result.get("success"):
                    print(f"[CONCURRENCY] {symbol}: {result.get('total_duration_ms', 0):.0f}ms")
                else:
                    print(f"[CONCURRENCY] {symbol}: FAILED - {result.get('error', 'unknown')}")
                    errors.append(result)

            except Exception as e:
                error_result = {"symbol": symbol, "success": False, "error": str(e)}
                errors.append(error_result)
                print(f"[CONCURRENCY] {symbol}: EXCEPTION - {e}")

    total_duration = time.time() - start_time

    # Calculate statistics
    successful_results = [r for r in results if r.get("success")]
    success_rate = len(successful_results) / len(symbols) if symbols else 0

    durations = [r.get("total_duration_ms", 0) for r in successful_results]
    avg_duration = sum(durations) / len(durations) if durations else 0
    max_duration = max(durations) if durations else 0
    min_duration = min(durations) if durations else 0

    return {
        "test_type": "concurrent_requests",
        "symbols_tested": len(symbols),
        "max_workers": max_workers,
        "total_duration_s": round(total_duration, 2),
        "success_rate": round(success_rate, 3),
        "successful_requests": len(successful_results),
        "failed_requests": len(errors),
        "performance_stats": {
            "avg_duration_ms": round(avg_duration, 2),
            "min_duration_ms": round(min_duration, 2),
            "max_duration_ms": round(max_duration, 2)
        },
        "results": results,
        "errors": errors
    }

def test_sustained_load(duration_seconds: int = 60, requests_per_second: int = 2) -> Dict[str, Any]:
    """Test sustained load over time to validate semaphore behavior"""
    print(f"[LOAD-TEST] Running sustained load test for {duration_seconds}s at {requests_per_second} req/s...")

    theta = ThetaDataClient()

    if not theta.ok():
        return {
            "success": False,
            "error": "ThetaData not available"
        }

    # Test symbols to cycle through
    test_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA", "META", "NFLX"]
    test_date = "2025-09-11"

    start_time = time.time()
    end_time = start_time + duration_seconds

    request_count = 0
    successful_requests = 0
    failed_requests = 0
    response_times = []

    interval = 1.0 / requests_per_second

    while time.time() < end_time:
        request_start = time.time()

        # Cycle through test symbols
        symbol = test_symbols[request_count % len(test_symbols)]

        try:
            # Simple EOD request
            result = theta.get_prev_close_eod(symbol, test_date)
            request_end = time.time()

            response_time = (request_end - request_start) * 1000
            response_times.append(response_time)

            if result is not None:
                successful_requests += 1
            else:
                failed_requests += 1

        except Exception as e:
            request_end = time.time()
            failed_requests += 1
            print(f"[LOAD-TEST] Request {request_count} failed: {e}")

        request_count += 1

        # Maintain request rate
        elapsed = time.time() - request_start
        sleep_time = max(0, interval - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    total_duration = time.time() - start_time
    actual_rps = request_count / total_duration

    # Calculate response time statistics
    if response_times:
        avg_response = sum(response_times) / len(response_times)
        max_response = max(response_times)
        min_response = min(response_times)
        response_times.sort()
        p95_response = response_times[int(0.95 * len(response_times))] if response_times else 0
    else:
        avg_response = max_response = min_response = p95_response = 0

    return {
        "test_type": "sustained_load",
        "duration_s": round(total_duration, 2),
        "target_rps": requests_per_second,
        "actual_rps": round(actual_rps, 2),
        "total_requests": request_count,
        "successful_requests": successful_requests,
        "failed_requests": failed_requests,
        "success_rate": round(successful_requests / request_count if request_count > 0 else 0, 3),
        "response_time_stats": {
            "avg_ms": round(avg_response, 2),
            "min_ms": round(min_response, 2),
            "max_ms": round(max_response, 2),
            "p95_ms": round(p95_response, 2)
        }
    }

def run_comprehensive_theta_validation() -> Dict[str, Any]:
    """Run comprehensive ThetaData performance validation"""
    print("=== ThetaData Concurrency & Performance Validation ===")

    validation_results = {
        "timestamp": datetime.now().isoformat(),
        "theta_version": None,
        "tests": {}
    }

    # Test 1: Basic connectivity
    connectivity_result = test_basic_connectivity()
    validation_results["theta_version"] = connectivity_result.get("version")
    validation_results["tests"]["connectivity"] = connectivity_result

    if not connectivity_result.get("theta_ok"):
        print("[ERROR] ThetaData not available - skipping performance tests")
        return validation_results

    # Test 2: Single request performance
    print("\n[PERFORMANCE] Testing single request performance...")
    theta = ThetaDataClient()
    single_result = test_single_request_performance(theta, "AAPL", "2025-09-11")
    validation_results["tests"]["single_request"] = single_result

    # Test 3: Concurrent requests (Standard tier limit: 4)
    test_symbols = ["AAPL", "GOOGL", "MSFT", "TSLA", "AMZN", "NVDA", "META", "NFLX", "AMD", "CRM"]
    concurrent_result = test_concurrent_requests(test_symbols, "2025-09-11", max_workers=4)
    validation_results["tests"]["concurrent_requests"] = concurrent_result

    # Test 4: Sustained load test
    print("\n[LOAD-TEST] Testing sustained load...")
    load_result = test_sustained_load(duration_seconds=30, requests_per_second=2)
    validation_results["tests"]["sustained_load"] = load_result

    # Generate summary
    print("\n=== Validation Summary ===")
    print(f"ThetaData Version: {validation_results.get('theta_version', 'Unknown')}")
    print(f"Single Request: {single_result.get('total_duration_ms', 0):.0f}ms")
    print(f"Concurrent Success Rate: {concurrent_result.get('success_rate', 0):.1%}")
    print(f"Load Test Success Rate: {load_result.get('success_rate', 0):.1%}")
    print(f"Load Test Avg Response: {load_result.get('response_time_stats', {}).get('avg_ms', 0):.0f}ms")

    # Save detailed results
    report_path = f"project_state/theta_performance_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs("project_state", exist_ok=True)

    with open(report_path, 'w', encoding='ascii', errors='replace') as f:
        json.dump(validation_results, f, indent=2, default=str)

    print(f"Detailed report saved to: {report_path}")

    return validation_results

def main():
    """Main CLI entry point for ThetaData performance validation"""
    parser = argparse.ArgumentParser(description="ThetaData concurrency and performance validation")
    parser.add_argument("--test", choices=["connectivity", "single", "concurrent", "load", "all"],
                        default="all", help="Test type to run")
    parser.add_argument("--symbols", type=int, default=10, help="Number of symbols for concurrent test")
    parser.add_argument("--duration", type=int, default=30, help="Duration for load test (seconds)")
    parser.add_argument("--rps", type=int, default=2, help="Requests per second for load test")

    args = parser.parse_args()

    if args.test == "all":
        result = run_comprehensive_theta_validation()
        exit_code = 0 if result["tests"].get("connectivity", {}).get("theta_ok") else 1
    else:
        print(f"Running {args.test} test only...")
        # Individual test implementations would go here
        exit_code = 0

    sys.exit(exit_code)

if __name__ == "__main__":
    main()