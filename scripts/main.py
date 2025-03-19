import json
import requests
from datetime import datetime
from scripts.__init__ import *
from dotenv import load_dotenv
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client
from clickhouse_connect.driver.query import QueryResult


load_dotenv()
logger = get_logger(str(os.path.basename(__file__).replace(".py", "_") + str(datetime.now().date())))
# NOT_COUNT_COMPANIES: list = [
#     "6162015019",
#     "2209006093",
#     "5752002910",
#     "3122000300",
#     "2201000766",
#     "7710002750",
#     "7113502396",
#     "3122503751",
#     "6167055777",
#     "6443007550",
#     "7743084941",
#     "3811185573"
# ]


class UpdatingCompanies:
    def __init__(self):
        pass

    @staticmethod
    def connect_to_db() -> list:
        """
        Connect to the database and retrieve data from the `reference_compass` table.
    
        This method establishes a connection to the database using environment variables for 
        host, database, username, and password. It queries the `reference_compass` table, 
        ordering the results by `last_updated` and `original_file_name`, and limits the results 
        to 19,000 rows. Each row of the query result is transformed into a dictionary 
        with keys corresponding to specific columns and additional metadata.
    
        :return: A list of dictionaries, each representing a row from the query result 
                  with fields for 'uuid', 'inn', 'dadata_status', 'dadata_branch_name', 
                  'dadata_branch_address', 'dadata_branch_region', 'last_updated', 
                  and 'from_cache'.
        :raises: If there is an error connecting to the database, logs the error and 
                 exits the program with a status of 1.
        """
        try:
            logger.info("Will connect to db")
            client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                        username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
            ref_compass: QueryResult = client.query(
                "SELECT * "
                "FROM reference_compass "
                "ORDER BY last_updated NULLS FIRST, original_file_name "
                "LIMIT 19000"
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
                    "from_cache": False
                 } for row in ref_compass.result_rows
            ]
        except Exception as ex_connect:
            logger.error(f"Error connection to db {ex_connect}. Type error is {type(ex_connect)}.")
            sys.exit(1)

    @staticmethod
    def write_to_json(parsed_data: list, index: int) -> None:
        """
        Writes the given parsed data to a JSON file.

        :param parsed_data: A list of dictionaries, where each dictionary represents a company
        :param index: The index of the file to write. The file name will be in the format
                      '{name}_{index}.json'
        :return: None
        """
        logger.info("Write data to json")
        name = "update"
        dir_name = f"{os.environ.get('XL_IDP_PATH_REFERENCE')}/reference_compass/{name}"
        os.makedirs(dir_name, exist_ok=True)
        output_file_path: str = os.path.join(dir_name, f'{name}_{index}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    @staticmethod
    def add_dadata_columns(
        company_data: dict,
        company_address: dict,
        company_address_data: dict,
        company_data_branch: str,
        company: dict,
        dict_data: dict,
        is_company_name_from_cache: bool
    ) -> None:
        """
        Add columns to the given dictionary with data from DaData.

        This method takes a dictionary of company data and adds columns based on the given branch type.
        If the branch type is MAIN, it adds columns for company name, OKPO, address, region, federal district,
        city, OKVED activity main type, geo latitude, and geo longitude. If the branch type is BRANCH,
        it adds columns for branch name with KPP, branch address, and branch region.

        :param company_data: A dictionary of company data
        :param company_address: A dictionary of company address
        :param company_address_data: A dictionary of company address data
        :param company_data_branch: The branch type
        :param company: A dictionary of company data
        :param dict_data: The dictionary to which columns should be added
        :param is_company_name_from_cache: Whether the company name is from cache
        :return: None
        """
        is_main: bool = company_data_branch == "MAIN" or not company_data_branch
        is_branch: bool = company_data_branch == "BRANCH"

        dict_data.update({
            "dadata_company_name": (
                f'{company_data.get("opf", {}).get("short", "")} '
                f'{company_data.get("name", {}).get("full", "")}'.strip()
            ) if is_main else dict_data.get("dadata_company_name"),
            "dadata_okpo": company_data.get("okpo") if is_main else dict_data.get("dadata_okpo"),
            "dadata_address": company_address.get("unrestricted_value") if is_main else dict_data.get("dadata_address"),
            "dadata_region": company_address_data.get("region_with_type") if is_main else dict_data.get("dadata_region"),
            "dadata_federal_district": company_address_data.get("federal_district")
                if is_main else dict_data.get("dadata_federal_district"),
            "dadata_city": company_address_data.get("city") if is_main else dict_data.get("dadata_city"),
            "dadata_okved_activity_main_type": company_data.get("okved")
                if is_main else dict_data.get("dadata_okved_activity_main_type"),
            "dadata_geo_lat": company_address_data.get("geo_lat") if is_main else dict_data.get("dadata_geo_lat"),
            "dadata_geo_lon": company_address_data.get("geo_lon") if is_main else dict_data.get("dadata_geo_lon"),
        })

        dict_data.update({
            "dadata_branch_name": f'{company.get("value", "")}, КПП {company_data.get("kpp", "")}\n' if is_branch else '',
            "dadata_branch_address": f'{company_address.get("unrestricted_value", "")}\n' if is_branch else '',
            "dadata_branch_region": f'{company_address_data.get("region_with_type", "")}\n' if is_branch else '',
        })

        dict_data["is_company_name_from_cache"] = is_company_name_from_cache

    @staticmethod
    def get_status(dict_data: dict, company_data: dict) -> None:
        """
        Get the status of the company.
        """
        dict_data["dadata_status"] = company_data["state"]["status"]
        dict_data["dadata_registration_date"] = \
            datetime.fromtimestamp(
                company_data["state"]["registration_date"] // 1000
            ).strftime('%Y-%m-%d') if company_data["state"]["registration_date"] else None
        dict_data["dadata_liquidation_date"] = \
            datetime.fromtimestamp(
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
                company_data_branch: str = company_data.get("branch_type")
                if company_data_branch == "MAIN":
                    self.get_status(dict_data, company_data)
                self.add_dadata_columns(
                    company_data, company_address, company_address_data, company_data_branch,
                    company, dict_data, dadata_request[1]
                )
            except Exception as ex_parse:
                logger.error(
                    f"Error code: error processing in row {index + 1}! "
                    f"Error is {ex_parse} Data is {dict_data}"
                )

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
            # if dict_data["inn"] not in NOT_COUNT_COMPANIES:
            self.get_data_from_service_inn(dict_data, i)
            self.write_to_json(dict_data, i)


if __name__ == "__main__":
    UpdatingCompanies().main()
