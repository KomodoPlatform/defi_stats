#!/usr/bin/env python3
"""
Utility functions for getting client IP addresses in FastAPI
Handles cases with and without proxies/load balancers
"""

from fastapi import Request


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