import pytest


@pytest.fixture
def sample_channel_details():
    return {
        "username": "testcreator",
        "display_name": "Test Creator",
        "bio_snippet": "A channel about testing things.",
        "email": "test@example.com",
        "country": "FR",
        "published_at": "2023-01-01T00:00:00Z",
        "followers": 25_000,
        "total_views": 500_000,
        "total_video_count": 100,
        "hidden_subscribers": False,
        "content_categories": ["Gaming", "Sports"],
        "channel_keywords": "sorare fantasy football",
    }


@pytest.fixture
def sample_search_data():
    return {
        "channel_id": "UC_TEST_123",
        "display_name": "Test Creator",
        "video_ids": ["v1", "v2", "v3"],
        "mentions_count": 5,
    }


@pytest.fixture
def sample_vstats():
    return {
        "views": 10_000,
        "likes": 500,
        "comments": 50,
        "video_count": 12,
        "shorts_count": 2,
        "long_form_count": 10,
        "per_video_views": [500, 600, 700, 800, 900, 1000, 1100, 1200, 1300, 1400, 1500, 1000],
        "is_chronological": True,
    }
