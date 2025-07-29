import requests
import json
import os

BASE_URL = "http://localhost:8000"

# Use a session to persist headers
session = requests.Session()

def run_test(description, func, *args, **kwargs):
    print(f"\n--- {description} ---")
    try:
        response = func(*args, **kwargs)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        print(f"SUCCESS: {response.status_code}")
        if response.content:
            try:
                print(json.dumps(response.json(), indent=2))
                return response.json()
            except json.JSONDecodeError:
                print(response.text)
                return response.text
        return None
    except requests.exceptions.HTTPError as e:
        print(f"FAILED: {e.response.status_code}")
        print(f"Error details: {e.response.text}")
        # In a real test suite, you might want to exit or handle this differently
        raise

def login(email, password):
    print(f"Attempting to log in as {email}...")
    response = session.post(f"{BASE_URL}/login", data={"username": email, "password": password})
    response.raise_for_status()
    token = response.json()["access_token"]
    session.headers.update({"Authorization": f"Bearer {token}"})
    print("Login successful.")
    return token

def main():
    admin_email = "admin@evergreen.com"
    admin_password = "adminpassword"
    
    partner_email = "testpartner@example.com"
    partner_password = "partnerpass123"
    new_partner_password = "new_password_456"
    
    created_podcast_id = None
    created_partner_id = None

    try:
        # 1. Admin Login
        print("\n--- Admin Login ---")
        login(admin_email, admin_password)

        # 2. Create Podcast (Admin)
        podcast_data = {
            "title": "The Test Chamber",
            "media_type": "audio",
            "show_type": "Original",
            "relationship_level": "strong",
        }
        created_podcast = run_test("Create Podcast", session.post, f"{BASE_URL}/podcasts", json=podcast_data)
        created_podcast_id = created_podcast['id']

        # 3. View All Podcasts (Admin)
        run_test("View All Podcasts", session.get, f"{BASE_URL}/podcasts")

        # 4. Update Podcast (Admin)
        update_data = {"tentpole": True}
        run_test("Update Podcast", session.put, f"{BASE_URL}/podcasts/{created_podcast_id}", json=update_data)

        # 5. Filter for the created podcast
        run_test("Filter Podcasts", session.get, f"{BASE_URL}/podcasts/filter", params={"title": "The Test Chamber"})

        # 6. Create Partner (Admin)
        partner_data = {"email": partner_email, "password": partner_password}
        created_partner = run_test("Create Partner", session.post, f"{BASE_URL}/partners", json=partner_data)
        created_partner_id = created_partner['id']

        # 7. Associate Partner with Podcast (Admin)
        run_test("Associate Partner with Podcast", session.post, f"{BASE_URL}/podcasts/{created_podcast_id}/partners/{created_partner_id}")

        # 8. Update Partner Password (Admin)
        password_data = {"new_password": new_partner_password}
        run_test("Update Partner Password", session.put, f"{BASE_URL}/partners/{created_partner_id}/password", json=password_data)

        # 9. Partner Login
        # Clear admin auth and log in as partner
        session.headers.pop("Authorization")
        run_test("Partner Login", login, partner_email, new_partner_password)

        # 10. Partner Views Their Podcasts
        run_test("Partner Views Their Podcasts", session.get, f"{BASE_URL}/partners/me/podcasts")

    except Exception as e:
        print(f"\nAn error occurred during the test run: {e}")
    finally:
        print("\n--- Starting Cleanup --- ")
        # Re-login as admin to perform cleanup
        try:
            print("\n--- Admin Re-Login for Cleanup ---")
            login(admin_email, admin_password)
            
            if created_podcast_id and created_partner_id:
                run_test("Unassociate Partner from Show", session.delete, f"{BASE_URL}/podcasts/{created_podcast_id}/partners/{created_partner_id}")
            
            if created_podcast_id:
                deleted_podcast_data = run_test("Fetch Deleted Podcast Data", session.get, f"{BASE_URL}/podcasts/{created_podcast_id}")
                run_test("Delete Podcast", session.delete, f"{BASE_URL}/podcasts/{created_podcast_id}")
                # Recreate the podcast to leave the DB in a consistent state
                recreate_data = {
                    "title": deleted_podcast_data.get("title"),
                    "media_type": deleted_podcast_data.get("media_type"),
                    "show_type": deleted_podcast_data.get("show_type"),
                    "relationship_level": deleted_podcast_data.get("relationship_level"),
                    "tentpole": deleted_podcast_data.get("tentpole"),
                }
                run_test("Recreate Deleted Podcast", session.post, f"{BASE_URL}/podcasts", json=recreate_data)
            
            if created_partner_id:
                run_test("Delete Partner", session.delete, f"{BASE_URL}/users/{created_partner_id}")

            print("\nCleanup complete.")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"An error occurred during cleanup: {e}")
            print("Manual cleanup may be required for the following resources:")
            print(f"- Podcast ID: {created_podcast_id}")
            print(f"- Partner ID: {created_partner_id}")

if __name__ == "__main__":
    main()
