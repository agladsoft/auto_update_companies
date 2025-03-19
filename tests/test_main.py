import os
import json
import pytest
from unittest.mock import patch, MagicMock

os.environ["XL_IDP_ROOT_AUTO_UPDATE_SCRIPTS"] = os.path.dirname(os.path.dirname(__file__))

from scripts.main import UpdatingCompanies


@pytest.fixture
def updater():
    return UpdatingCompanies()


@patch("scripts.main.get_client")
def test_connect_to_db(mock_get_client, updater):
    mock_client = MagicMock()
    mock_client.query.return_value.result_rows = [
        ("uuid-123", "1234567890") + ("",) * 30 + ("ACTIVE",)  # Добавляем пустые строки, чтобы заполнить до 33
    ]
    mock_get_client.return_value = mock_client

    result = updater.connect_to_db()
    assert len(result) == 1
    assert result[0]["uuid"] == "uuid-123"
    assert result[0]["inn"] == "1234567890"
    assert result[0]["dadata_status"] == "ACTIVE"


@pytest.mark.parametrize("parsed_data, index", [
    ([{"uuid": "uuid-123", "inn": "1234567890"}], 0)
])
def test_write_to_json(updater, parsed_data, index, tmpdir):
    output_dir = tmpdir.mkdir("reference_compass")
    with patch("os.environ.get", return_value=str(tmpdir)):
        updater.write_to_json(parsed_data, index)

    output_file = output_dir.join("update/update_0.json")
    assert output_file.check()
    with output_file.open("r", encoding="utf-8") as f:
        data = json.load(f)
        assert data[0]["uuid"] == "uuid-123"
        assert data[0]["inn"] == "1234567890"


@pytest.mark.parametrize(
    "company_data, expected_status, expected_registration_date, expected_liquidation_date",
    [
        (
            {"state": {"status": "active", "registration_date": 1633024800000, "liquidation_date": None}},
            "active",
            "2021-09-30",
            None
        ),
        (
            {"state": {"status": "liquidated", "registration_date": None, "liquidation_date": 1633024800000}},
            "liquidated",
            None,
            "2021-09-30"
        ),
    ]
)
def test_get_status(updater, company_data, expected_status, expected_registration_date, expected_liquidation_date):
    dict_data = {}
    updater.get_status(dict_data, company_data)

    assert dict_data["dadata_status"] == expected_status
    assert dict_data["dadata_registration_date"] == expected_registration_date
    assert dict_data["dadata_liquidation_date"] == expected_liquidation_date


@pytest.mark.parametrize("response_data", [
    ([{
        "data": {
            "name": {"full": "Test Company"},
            "branch_type": "MAIN",
            "state": {
                "status": "ACTIVE",
                "registration_date": 1633046400000,
                "liquidation_date": None
            },
            "address": {
                "unrestricted_value": "Test address",
                "data": {
                    "region_with_type": "Test region",
                    "federal_district": "Test district",
                    "city": "Test city",
                    "geo_lat": 12.34,
                    "geo_lon": 56.78
                }
            },
            "opf": {"short": "OOO"},
            "okpo": "1234567890",
            "okved": "12345"
        }
    }], False
    )
])
@patch("requests.post")
def test_get_data_from_service_inn(mock_post, updater, response_data):
    mock_response = MagicMock()
    mock_response.json.return_value = response_data
    mock_response.status_code = 200
    mock_post.return_value = mock_response

    dict_data = {
        "inn": "1234567890",
        "dadata_branch_name": '',
        "dadata_branch_address": '',
        "dadata_branch_region": ''
    }
    updater.get_data_from_service_inn(dict_data, 0)
    assert dict_data["dadata_status"] == "ACTIVE"
    assert dict_data["dadata_registration_date"] == "2021-10-01"
    assert dict_data["dadata_liquidation_date"] is None


@pytest.mark.parametrize(
    "company_data, company_address, company_address_data, company_data_branch, company, is_company_name_from_cache, expected_dict_data",
    [
        # Happy path: MAIN branch
        (
            {"opf": {"short": "OOO"}, "name": {"full": "Test Company"}, "okpo": "1234567890", "okved": "12345"},
            {"unrestricted_value": "Test address"},
            {
                "region_with_type": "Test region", "federal_district": "Test district",
                "city": "Test city", "geo_lat": 12.34, "geo_lon": 56.78
            },
            "MAIN",
            {},
            False,
            {
                "dadata_company_name": "OOO Test Company", "dadata_okpo": "1234567890",
                "dadata_address": "Test address", "dadata_region": "Test region",
                "dadata_federal_district": "Test district", "dadata_city": "Test city",
                "dadata_okved_activity_main_type": "12345", "dadata_branch_name": "", "dadata_branch_address": "",
                "dadata_branch_region": "", "dadata_geo_lat": 12.34, "dadata_geo_lon": 56.78,
                "is_company_name_from_cache": False
            }
        ),
        # Happy path: BRANCH branch
        (
            {"opf": {"short": "OOO"}, "name": {"full": "Branch Name"}, "kpp": "kpp_value", "okved": "12345"},
            {"unrestricted_value": "Branch address"},
            {"region_with_type": "Branch region"},
            "BRANCH",
            {"value": "Branch Value"},
            True,
            {
                "dadata_company_name": None, "dadata_okpo": None, "dadata_address": None, "dadata_region": None,
                "dadata_federal_district": None, "dadata_city": None, "dadata_okved_activity_main_type": None,
                "dadata_branch_name": "Branch Value, КПП kpp_value\n", "dadata_branch_address": "Branch address\n",
                "dadata_branch_region": "Branch region\n", "dadata_geo_lat": None, "dadata_geo_lon": None,
                "is_company_name_from_cache": True
            }
        ),
        # Edge case: Missing opf.short
        (
            {"opf": {}, "name": {"full": "Test Company"}},
            {"unrestricted_value": "Test address"},
            {"region_with_type": "Test region"},
            "MAIN",
            {},
            False,
            {
                "dadata_company_name": "Test Company", "dadata_okpo": None, "dadata_address": "Test address",
                "dadata_region": "Test region", "dadata_federal_district": None, "dadata_city": None,
                "dadata_okved_activity_main_type": None, "dadata_branch_name": "", "dadata_branch_address": "",
                "dadata_branch_region": "", "dadata_geo_lat": None, "dadata_geo_lon": None,
                "is_company_name_from_cache": False
            }
        ),
        # Edge case: Missing name.full
        (
            {"opf": {"short": "ООО"}},
            {"unrestricted_value": "Test address"},
            {"region_with_type": "Test region"},
            "MAIN",
            {},
            False,
            {
                "dadata_company_name": "ООО", "dadata_okpo": None, "dadata_address": "Test address",
                "dadata_region": "Test region", "dadata_federal_district": None, "dadata_city": None,
                "dadata_okved_activity_main_type": None, "dadata_branch_name": "", "dadata_branch_address": "",
                "dadata_branch_region": "", "dadata_geo_lat": None, "dadata_geo_lon": None,
                "is_company_name_from_cache": False
            }

        ),
        # Edge case: Empty input
        (
            {}, {}, {}, "MAIN", {}, False,
            {
                "dadata_company_name": "", "dadata_okpo": None, "dadata_address": None, "dadata_region": None,
                "dadata_federal_district": None, "dadata_city": None, "dadata_okved_activity_main_type": None,
                "dadata_branch_name": "", "dadata_branch_address": "", "dadata_branch_region": "",
                "dadata_geo_lat": None, "dadata_geo_lon": None, "is_company_name_from_cache": False
            }
        ),

    ],
    ids=["happy_path_main", "happy_path_branch", "missing_opf_short", "missing_name_full", "empty_input"]
)
def test_add_dadata_columns(
    updater,
    company_data,
    company_address,
    company_address_data,
    company_data_branch,
    company,
    is_company_name_from_cache,
    expected_dict_data
):
    # Act
    dict_data = {
        "dadata_branch_name": '',
        "dadata_branch_address": '',
        "dadata_branch_region": ''
    }
    updater.add_dadata_columns(
        company_data,
        company_address,
        company_address_data,
        company_data_branch,
        company,
        dict_data,
        is_company_name_from_cache
    )

    # Assert
    assert dict_data == expected_dict_data


