import json
import requests
from __init__ import *
from datetime import datetime
from dotenv import load_dotenv
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import QueryResult


load_dotenv()
logger = get_logger(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date()))


class UpdatingCompanies:
    def __init__(self):
        pass

    @staticmethod
    def connect_to_db() -> list:
        """
        Connecting to clickhouse.
        """
        try:
            logger.info("Will connect to db")
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            ref_compass: QueryResult = client.query(
                "SELECT * "
                "FROM reference_compass "
            )
            # Чтобы проверить, есть ли данные. Так как переменная образуется, но внутри нее могут быть ошибки.
            print(ref_compass.result_rows[0])
            logger.info("Connected to db")
            start_time: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            return [
                {
                    "uuid": str(row[0]),
                    "inn": row[1],
                    "dadata_status": row[32],
                    "dadata_branch_name": '',
                    "dadata_branch_address": '',
                    "dadata_branch_region": '',
                    "last_updated": start_time,
                    "from_cache": True
                 } for row in ref_compass.result_rows
            ]
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            sys.exit(1)

    @staticmethod
    def write_to_json(parsed_data: list, index: int) -> None:
        """
        Write data to json.
        """
        logger.info("Write data to json")
        name = "update"
        dir_name = f"{os.environ.get('XL_IDP_PATH_REFERENCE')}/reference_compass/{name}"
        os.makedirs(dir_name, exist_ok=True)
        output_file_path: str = os.path.join(dir_name, f'{name}_{index}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    @staticmethod
    def add_dadata_columns(company_data: dict, company_address: dict, company_address_data: dict,
                           company_data_branch: dict, company: dict, dict_data: dict,
                           is_company_name_from_cache: bool) -> None:
        """
        Add values from dadata to the dictionary.
        """
        dict_data["dadata_company_name"] = \
            f'{company_data.get("opf").get("short", "") if company_data.get("opf") else ""} ' \
            f'{company_data["name"]["full"]}'.strip() \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_company_name"]
        dict_data["dadata_okpo"] = company_data.get("okpo") \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_okpo"]
        dict_data["dadata_address"] = company_address.get("unrestricted_value") \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_address"]
        dict_data["dadata_region"] = company_address_data.get("region_with_type") \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_region"]
        dict_data["dadata_federal_district"] = company_address_data.get("federal_district") \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_federal_district"]
        dict_data["dadata_city"] = company_address_data.get("city") \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_city"]
        dict_data["dadata_okved_activity_main_type"] = company_data.get("okved") \
            if company_data_branch == "MAIN" or not company_data_branch \
            else dict_data["dadata_okved_activity_main_type"]
        dict_data["dadata_branch_name"] += f'{company.get("value")}, КПП {company_data.get("kpp", "")}' + '\n' \
            if company_data_branch == "BRANCH" else ''
        dict_data["dadata_branch_address"] += company_address["unrestricted_value"] + '\n' \
            if company_data_branch == "BRANCH" else ''
        dict_data["dadata_branch_region"] += company_address_data["region_with_type"] + '\n' \
            if company_data_branch == "BRANCH" else ''
        dict_data["dadata_geo_lat"] = company_address_data.get("geo_lat") \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_geo_lat"]
        dict_data["dadata_geo_lon"] = company_address_data.get("geo_lon") \
            if company_data_branch == "MAIN" or not company_data_branch else dict_data["dadata_geo_lat"]
        dict_data["is_company_name_from_cache"] = is_company_name_from_cache

    @staticmethod
    def get_status(dict_data: dict, company_data: dict):
        """
        Get the status of the company.
        """
        dict_data["dadata_status"] = company_data["state"]["status"]
        dict_data["dadata_registration_date"] = \
            datetime.utcfromtimestamp(
                company_data["state"]["registration_date"] // 1000
            ).strftime('%Y-%m-%d') if company_data["state"]["registration_date"] else None
        dict_data["dadata_liquidation_date"] = \
            datetime.utcfromtimestamp(
                company_data["state"]["liquidation_date"] // 1000
            ).strftime('%Y-%m-%d') if company_data["state"]["liquidation_date"] else None

    def get_data_from_dadata(self, dadata_request: list, dict_data: dict, index: int) -> None:
        """
        Get data from dadata.
        """
        for company in dadata_request[0]:
            try:
                company_data: dict = company.get("data")
                company_address: dict = company_data.get("address") or {}
                company_address_data: dict = company_address.get("data", {})
                company_data_branch: dict = company_data.get("branch_type")
                if company_data_branch == "MAIN":
                    self.get_status(dict_data, company_data)
                self.add_dadata_columns(company_data, company_address, company_address_data, company_data_branch,
                                        company, dict_data, dadata_request[1])
            except Exception as ex_parse:
                logger.error(f"Error code: error processing in row {index + 1}! "
                             f"Error is {ex_parse} Data is {dict_data}")

    def get_data_from_service_inn(self, dict_data: dict, index: int) -> None:
        """
        Connect to dadata.
        """
        try:
            response: requests.Response = requests.post(f"http://{get_my_env_var('SERVICE_INN')}:8003", json=dict_data)
            response.raise_for_status()
            self.get_data_from_dadata(response.json(), dict_data, index)
            dict_data["dadata_branch_name"] = dict_data["dadata_branch_name"] or None
            dict_data["dadata_branch_address"] = dict_data["dadata_branch_address"] or None
            dict_data["dadata_branch_region"] = dict_data["dadata_branch_region"] or None
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred during the API request: {str(e)}")

    def main(self):
        list_inn = self.connect_to_db()
        for i, dict_data in enumerate(list_inn):
            self.get_data_from_service_inn(dict_data, i)
            self.write_to_json(dict_data, i)


if __name__ == "__main__":
    UpdatingCompanies().main()
