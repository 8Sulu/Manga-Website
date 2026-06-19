import os
import requests
from dotenv import load_dotenv

# Load original environment variables
load_dotenv()


def update_env_file(new_access, new_refresh):
    """Safely updates only the token values in the .env file without wiping other variables."""
    lines = []
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            lines = f.readlines()

    updated_lines = []
    has_access = False
    has_refresh = False

    for line in lines:
        if line.startswith("MAL_ACCESS_TOKEN="):
            updated_lines.append(f'MAL_ACCESS_TOKEN="{new_access}"\n')
            has_access = True
        elif line.startswith("MAL_REFRESH_TOKEN="):
            updated_lines.append(f'MAL_REFRESH_TOKEN="{new_refresh}"\n')
            has_refresh = True
        else:
            updated_lines.append(line)

    if not has_access:
        updated_lines.append(f'MAL_ACCESS_TOKEN="{new_access}"\n')
    if not has_refresh:
        updated_lines.append(f'MAL_REFRESH_TOKEN="{new_refresh}"\n')

    with open(".env", "w") as f:
        f.writelines(updated_lines)


def refresh_tokens():
    """Hits the MAL OAuth2 endpoint to exchange the refresh token for new access/refresh tokens."""
    print("[*] Access token expired. Attempting to refresh tokens...")
    url = "https://myanimelist.net/v1/oauth2/token"

    data = {
        "client_id": os.getenv("MAL_CLIENT_ID"),
        "client_secret": os.getenv("MAL_CLIENT_SECRET"),
        "grant_type": "refresh_token",
        "refresh_token": os.getenv("MAL_REFRESH_TOKEN"),
    }

    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=data, headers=headers)

    if response.status_code == 200:
        tokens = response.json()
        new_access = tokens["access_token"]
        new_refresh = tokens["refresh_token"]

        # Write to file and update current in-memory environment properties
        update_env_file(new_access, new_refresh)
        os.environ["MAL_ACCESS_TOKEN"] = new_access
        os.environ["MAL_REFRESH_TOKEN"] = new_refresh
        print("[+] Tokens successfully refreshed and written to .env")
        return new_access
    else:
        print(f"[-] Critical: Failed to refresh token. Server returned {response.status_code}")
        print(response.text)
        return None


def authenticated_request(url, method="GET", **kwargs):
    """Wrapper that signs requests with Bearer tokens and handles automatic retries on 401s."""
    access_token = os.getenv("MAL_ACCESS_TOKEN")

    headers = kwargs.get("headers", {})
    headers["Authorization"] = f"Bearer {access_token}"
    kwargs["headers"] = headers

    response = requests.request(method, url, **kwargs)

    if response.status_code == 401:
        try:
            error_data = response.json()
            if error_data.get("error") == "invalid_token":
                new_token = refresh_tokens()
                if new_token:
                    # Retry once with the brand new access token
                    headers["Authorization"] = f"Bearer {new_token}"
                    kwargs["headers"] = headers
                    return requests.request(method, url, **kwargs)
        except ValueError:
            # Response wasn't valid JSON, return original 401 error
            pass

    return response
