import requests
from requests.auth import HTTPBasicAuth


def get_schema_from_registry(
        schema_registry_details: dict,
        user_id: str,
        workload_password: str
) -> dict:
    """
    Retrieve the latest schema from the schema registry.
    
    :param schema_registry_details: dict having schema registry details.
    :param user_id: User ID.
    :param workload_password: The workload password of the user.
    :return: The schema json object.
    """
    sr_subject = schema_registry_details["subject"]
    sr_url = schema_registry_details["url"]

    # TODO: TO BE REMOVED AS PART OF NEXT SPRINT
    # temporary solution to use no_proxy here
    no_proxy = "eks.eu-west-1.amazonaws.com,\
        autoscaling.eu-west-1.amazonaws.com,\
        cloudformation.eu-west-1.amazonaws.com,.boicssp.net,.cloudera.site"
    no_proxy_domains = no_proxy.split(",")
    proxies = {
        "http": None,
        "https": None,
        "no_proxy": ",".join(no_proxy_domains)
    }

    response = requests.get(
        f"{sr_url}/schemaregistry/schemas/"
        f"{sr_subject}/versions/latest/schemaText", 
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        auth=HTTPBasicAuth(user_id, workload_password),
        proxies=proxies,
        verify=False,
        timeout=100
    )

    response.raise_for_status()
    return response.json()

def get_schema_from_registry_1(
        schema_registry_details: dict,
        user_id: str,
        workload_password: str
) -> dict:
    """
    Retrieve the latest schema from the schema registry.
    
    :param schema_registry_details: dict having schema registry details.
    :param user_id: User ID.
    :param workload_password: The workload password of the user.
    :return: The schema json object.
    """
    sr_subject = schema_registry_details["subject"]
    sr_url = schema_registry_details["url"]

    # TODO: TO BE REMOVED AS PART OF NEXT SPRINT
    # temporary solution to use no_proxy here
    no_proxy = "eks.eu-west-1.amazonaws.com,\
        autoscaling.eu-west-1.amazonaws.com,\
        cloudformation.eu-west-1.amazonaws.com,.boicssp.net,.cloudera.site"
    no_proxy_domains = no_proxy.split(",")
    proxies = {
        "http": None,
        "https": None,
        "no_proxy": ",".join(no_proxy_domains)
    }

    response = requests.get(
        f"http://localhost:8081/subjects/Account_MonetaryAmount_BKK_DbtshpDbkacp_raw_avro/versions/1", 
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        auth=HTTPBasicAuth(user_id, workload_password),
        proxies=proxies,
        verify=False,
        timeout=100
    )

    response.raise_for_status()
    return response.json()