#!/usr/bin/env python3
"""
Utility functions for getting client IP addresses in FastAPI
Handles cases with and without proxies/load balancers
"""

from fastapi import Request
from typing import Optional


def get_client_ip(request: Request) -> str:
    """
    Get the client IP address from a FastAPI Request object.
    Handles cases where the application is behind a proxy or load balancer.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        str: Client IP address
    """
    # Check for X-Forwarded-For header (common with proxies/load balancers)
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        # X-Forwarded-For can contain multiple IPs, the first one is usually the client
        return forwarded_for.split(",")[0].strip()
    
    # Check for X-Real-IP header (used by some proxies like nginx)
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    
    # Check for CF-Connecting-IP header (Cloudflare)
    cf_ip = request.headers.get("CF-Connecting-IP")
    if cf_ip:
        return cf_ip.strip()
    
    # Fallback to direct client IP
    client_host = request.client.host if request.client else "unknown"
    return client_host


def get_all_ip_headers(request: Request) -> dict:
    """
    Get all IP-related headers for debugging purposes.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        dict: Dictionary containing all IP-related information
    """
    return {
        "client_host": request.client.host if request.client else None,
        "client_port": request.client.port if request.client else None,
        "x_forwarded_for": request.headers.get("X-Forwarded-For"),
        "x_real_ip": request.headers.get("X-Real-IP"),
        "cf_connecting_ip": request.headers.get("CF-Connecting-IP"),
        "x_forwarded_proto": request.headers.get("X-Forwarded-Proto"),
        "user_agent": request.headers.get("User-Agent"),
        "resolved_ip": get_client_ip(request)
    }


def is_private_ip(ip: str) -> bool:
    """
    Check if an IP address is in a private range.
    
    Args:
        ip: IP address string
        
    Returns:
        bool: True if IP is private, False otherwise
    """
    try:
        import ipaddress
        ip_obj = ipaddress.ip_address(ip)
        return ip_obj.is_private
    except (ValueError, ipaddress.AddressValueError):
        return False


def get_client_location(ip: str) -> Optional[dict]:
    """
    Get location information for an IP address using IP2Location.
    
    Args:
        ip: IP address string
        
    Returns:
        dict: Location information or None if not available
    """
    try:
        import IP2Location
        
        # You'll need to set the path to your IP2Location database file
        # Download from: https://lite.ip2location.com/
        database_path = "IP2LOCATION-LITE-DB1.BIN"  # Update this path
        
        database = IP2Location.IP2Location(database_path)
        result = database.get_all(ip)
        
        return {
            "ip": ip,
            "country_short": result.country_short,
            "country_long": result.country_long,
            "region": result.region,
            "city": result.city,
            "latitude": result.latitude,
            "longitude": result.longitude,
        }
    except Exception as e:
        # Return None if IP2Location database is not available or other error
        return None 