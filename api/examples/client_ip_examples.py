#!/usr/bin/env python3
"""
Examples of how to get client IP addresses in FastAPI routes
"""

from fastapi import APIRouter, Request, Depends
from fastapi.responses import JSONResponse
from util.client_ip import get_client_ip

# Example router (not included in main app)
examples_router = APIRouter()


# Method 1: Direct usage in route function
@examples_router.get("/example1")
def example_direct_ip(request: Request):
    """Direct usage of get_client_ip in route function"""
    client_ip = get_client_ip(request)
    return {"client_ip": client_ip, "method": "direct"}


# Method 2: Using FastAPI dependency injection
def get_client_ip_dependency(request: Request) -> str:
    """Dependency function to inject client IP"""
    return get_client_ip(request)


@examples_router.get("/example2")
def example_dependency_ip(client_ip: str = Depends(get_client_ip_dependency)):
    """Using dependency injection for client IP"""
    return {"client_ip": client_ip, "method": "dependency"}


# Method 3: Route with IP-based logic
@examples_router.get("/example3")
def example_ip_logic(request: Request):
    """Example with IP-based business logic"""
    client_ip = get_client_ip(request)
    
    # Example business logic: simple allow/block list
    blocked_ips = {"10.0.0.1", "192.168.1.100"}
    if client_ip in blocked_ips:
        message = "Access denied for this IP."
    else:
        message = "Access granted."
    
    return {
        "client_ip": client_ip,
        "message": message
    }


# Method 4: Middleware-style approach (for logging, analytics, etc.)
@examples_router.get("/example4")
def example_with_logging(request: Request):
    """Example that logs client IP for analytics"""
    client_ip = get_client_ip(request)
    
    # Log the request (you could save to database, send to analytics service, etc.)
    print(f"Request from IP: {client_ip}, Path: {request.url.path}")
    
    return {"message": "Request logged", "client_ip": client_ip}


# Method 5: Rate limiting by IP (conceptual example)
request_counts = {}  # In production, use Redis or proper cache

@examples_router.get("/example5")
def example_rate_limiting(request: Request):
    """Conceptual example of IP-based rate limiting"""
    client_ip = get_client_ip(request)
    
    # Simple in-memory rate limiting (use Redis in production)
    current_count = request_counts.get(client_ip, 0)
    
    if current_count >= 10:  # Max 10 requests per IP
        return JSONResponse(
            status_code=429, 
            content={"error": "Rate limit exceeded", "client_ip": client_ip}
        )
    
    request_counts[client_ip] = current_count + 1
    
    return {
        "message": "Request successful", 
        "client_ip": client_ip,
        "requests_count": request_counts[client_ip]
    } 